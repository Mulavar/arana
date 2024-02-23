/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dataset

import (
	"io"
	"sync"
)

import (
	"github.com/pkg/errors"

	"github.com/spf13/cast"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Dataset = (*SortMergeJoinDataSet)(nil)

const (
	IsDescartes  = true
	NotDescartes = false
)

// SortMergeJoinDataSet assume all leftDs data and rightDs data are sorted by join Column
type SortMergeJoinDataSet struct {
	// fields is the union of leftDs fields and rightDs fields
	fields []proto.Field
	// now only support one Column join
	joinColumn *JoinColumn
	// joinType rightDs join, left join, right join
	joinType ast.JoinType
	leftDs   proto.Dataset
	rightDs  proto.Dataset

	lastRow      proto.Row
	lastInnerRow proto.Row
	nextOuterRow proto.Row

	// equalValue when leftDs value equal rightDs value, set this value use to generate descartes product
	equalValue map[string][]proto.Row
	// equalIndex record the index of equalValue visited position
	equalIndex map[string]int
	// descartesFlag
	descartesFlag atomic.Bool
	rwLock        sync.RWMutex
}

func NewSortMergeJoinDataSet(joinType ast.JoinType, joinColumn *JoinColumn, leftDs proto.Dataset, rightDs proto.Dataset) (*SortMergeJoinDataSet, error) {
	leftFields, err := leftDs.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	rightFields, err := rightDs.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields := make([]proto.Field, 0, len(leftFields)+len(rightFields))
	fields = append(fields, leftFields...)
	fields = append(fields, rightFields...)

	if joinType == ast.RightJoin {
		leftDs, rightDs = rightDs, leftDs
		joinColumn.Column, joinColumn.RightColumn = joinColumn.RightColumn, joinColumn.Column
	}

	return &SortMergeJoinDataSet{
		fields:     fields,
		joinColumn: joinColumn,
		joinType:   joinType,
		leftDs:     leftDs,
		rightDs:    rightDs,
		equalValue: make(map[string][]proto.Row),
		equalIndex: make(map[string]int),
	}, nil
}

func (s *SortMergeJoinDataSet) SetEqualValue(key string, value proto.Row) {
	if s == nil {
		return
	}

	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.equalValue[key] = append(s.equalValue[key], value)
	if _, ok := s.equalIndex[key]; !ok {
		s.equalIndex[key] = 0
	}
}

func (s *SortMergeJoinDataSet) EqualValue(key string) proto.Row {
	if s == nil {
		return nil
	}

	s.rwLock.RLock()
	defer s.rwLock.RUnlock()

	if v, ok := s.equalValue[key]; ok {
		res := v[s.equalIndex[key]]
		index := s.equalIndex[key] + 1
		if index < len(v) {
			s.equalIndex[key] = index
		} else {
			s.equalIndex[key] = 0
		}
		return res
	}
	return nil
}

func (s *SortMergeJoinDataSet) EqualValueLen(key string) int {
	if s != nil {
		s.rwLock.RLock()
		defer s.rwLock.RUnlock()

		if v, ok := s.equalValue[key]; ok {
			return len(v)
		}
	}

	return 0
}

func (s *SortMergeJoinDataSet) EqualIndex(key string) int {
	if s != nil {
		s.rwLock.RLock()
		defer s.rwLock.RUnlock()

		if v, ok := s.equalIndex[key]; ok {
			return v
		}
	}

	return 0
}

func (s *SortMergeJoinDataSet) isDescartes(key string) bool {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if _, ok := s.equalValue[key]; ok {
		return true
	}

	return false
}

func (s *SortMergeJoinDataSet) setDescartesFlag(flag bool) {
	if s != nil {
		s.descartesFlag.Store(flag)
	}
}

func (s *SortMergeJoinDataSet) DescartesFlag() bool {
	if s != nil {
		return s.descartesFlag.Load()
	}

	return false
}

type JoinColumn struct {
	Column      string
	RightColumn string
}

func (s *SortMergeJoinDataSet) Close() error {
	return nil
}

func (s *SortMergeJoinDataSet) Fields() ([]proto.Field, error) {
	return s.fields, nil
}

func (s *SortMergeJoinDataSet) Next() (proto.Row, error) {
	var (
		err                error
		outerRow, innerRow proto.Row
	)

	if s.lastRow != nil {
		outerRow = s.lastRow
	} else {
		outerRow, err = s.getOuterRow()
		if err != nil {
			return nil, err
		}
	}

	innerRow, err = s.getInnerRow(outerRow)
	if err != nil {
		return nil, err
	}

	switch s.joinType {
	case ast.InnerJoin:
		return s.innerJoin(outerRow, innerRow)
	case ast.LeftJoin, ast.RightJoin:
		return s.outerJoin(outerRow, innerRow)
	default:
		return nil, errors.New("not support join type")
	}
}

// innerJoin
func (s *SortMergeJoinDataSet) innerJoin(leftRow proto.Row, rightRow proto.Row) (proto.Row, error) {
	var (
		err                   error
		leftValue, rightValue proto.Value
	)

	for {
		if leftRow == nil || rightRow == nil {
			return nil, io.EOF
		}

		leftValue, err = leftRow.(proto.KeyedRow).Get(s.joinColumn.Column)
		if err != nil {
			return nil, err
		}

		rightValue, err = rightRow.(proto.KeyedRow).Get(s.joinColumn.RightColumn)
		if err != nil {
			return nil, err
		}

		if proto.CompareValue(leftValue, rightValue) == 0 {
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			if res, err := s.equalCompare(leftRow, rightRow, leftValue); err != nil {
				return nil, err
			} else {
				return res, nil
			}
		}

		if proto.CompareValue(leftValue, rightValue) < 0 {
			s.lastRow = nil
			leftRow, err = s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if leftRow == nil {
				return nil, io.EOF
			}

			// if leftDs row equal last row, do descartes match
			leftValue, err = leftRow.(proto.KeyedRow).Get(s.joinColumn.Column)
			if err != nil {
				return nil, err
			}

			s.setDescartesFlag(NotDescartes)
			if s.isDescartes(leftValue.String()) {
				s.setDescartesFlag(IsDescartes)
				rightRow = s.EqualValue(leftValue.String())
			}
		}

		if proto.CompareValue(leftValue, rightValue) > 0 {
			leftRow, rightRow, err = s.greaterCompare(leftRow)
			if err != nil {
				return nil, err
			}

			if leftRow == nil {
				return nil, io.EOF
			}
		}
	}
}

// outerJoin
func (s *SortMergeJoinDataSet) outerJoin(leftRow proto.Row, rightRow proto.Row) (proto.Row, error) {
	var (
		err                    error
		outerValue, innerValue proto.Value
	)

	for {
		if leftRow == nil {
			return nil, io.EOF
		}

		if rightRow == nil {
			s.lastRow = nil
			outerValue, err = leftRow.(proto.KeyedRow).Get(s.joinColumn.Column)
			if err != nil {
				return nil, err
			}

			if s.isDescartes(outerValue.String()) {
				leftRow, err = s.getOuterRow()
				if err != nil {
					return nil, err
				}
				continue
			}

			return s.resGenerate(leftRow, nil), nil
		}

		outerValue, err = leftRow.(proto.KeyedRow).Get(s.joinColumn.Column)
		if err != nil {
			return nil, err
		}

		innerValue, err = rightRow.(proto.KeyedRow).Get(s.joinColumn.Column)
		if err != nil {
			return nil, err
		}

		if proto.CompareValue(outerValue, innerValue) == 0 {
			if res, err := s.equalCompare(leftRow, rightRow, outerValue); err != nil {
				return nil, err
			} else {
				return res, nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.lastRow = nil
			nextOuterRow, err := s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if nextOuterRow != nil {
				// if leftDs row equal last row, do descartes match
				nextOuterValue, err := nextOuterRow.(proto.KeyedRow).Get(s.joinColumn.Column)
				if err != nil {
					return nil, err
				}

				s.setDescartesFlag(NotDescartes)
				// record last rightDs row
				s.lastInnerRow = rightRow
				if s.isDescartes(nextOuterValue.String()) {
					s.setDescartesFlag(IsDescartes)
					rightRow = s.EqualValue(nextOuterValue.String())
					leftRow = nextOuterRow
				} else {
					if s.isDescartes(outerValue.String()) {
						if proto.CompareValue(nextOuterValue, innerValue) == 0 {
							s.lastInnerRow = nil
							leftRow = nextOuterRow
						} else {
							return s.resGenerate(nextOuterRow, nil), nil
						}
					} else {
						s.nextOuterRow = nextOuterRow
						return s.resGenerate(leftRow, nil), nil
					}
				}
			} else {
				if !s.isDescartes(outerValue.String()) {
					return s.resGenerate(leftRow, nil), nil
				}
				return nil, nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) > 0 {
			leftRow, rightRow, err = s.greaterCompare(leftRow)
			if err != nil {
				return nil, err
			}

			if leftRow == nil {
				return nil, io.EOF
			}
		}
	}
}

func (s *SortMergeJoinDataSet) getOuterRow() (proto.Row, error) {
	nextOuterRow := s.nextOuterRow
	if nextOuterRow != nil {
		s.nextOuterRow = nil
		return nextOuterRow, nil
	}

	leftRow, err := s.leftDs.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return leftRow, nil
}

func (s *SortMergeJoinDataSet) getInnerRow(outerRow proto.Row) (proto.Row, error) {
	if outerRow != nil {
		outerValue, err := outerRow.(proto.KeyedRow).Get(s.joinColumn.Column)
		if err != nil {
			return nil, err
		}

		if s.DescartesFlag() {
			innerRow := s.EqualValue(outerValue.String())
			if innerRow != nil {
				return innerRow, nil
			}
		}
	}

	lastInnerRow := s.lastInnerRow
	if lastInnerRow != nil {
		s.lastInnerRow = nil
		return lastInnerRow, nil
	}

	rightRow, err := s.rightDs.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return rightRow, nil
}

func (s *SortMergeJoinDataSet) resGenerate(leftRow proto.Row, rightRow proto.Row) proto.Row {
	var (
		leftValue  []proto.Value
		rightValue []proto.Value
		res        []proto.Value
	)

	if leftRow == nil && rightRow == nil {
		return nil
	}

	leftFields, _ := s.leftDs.Fields()
	rightFields, _ := s.rightDs.Fields()

	leftValue = make([]proto.Value, len(leftFields))
	rightValue = make([]proto.Value, len(rightFields))

	if leftRow == nil {
		if err := rightRow.(proto.KeyedRow).Scan(rightValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	if rightRow == nil {
		if err := leftRow.(proto.KeyedRow).Scan(leftValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	if leftRow != nil && rightRow != nil {
		if err := leftRow.(proto.KeyedRow).Scan(leftValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
		if err := rightRow.(proto.KeyedRow).Scan(rightValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	res = append(res, leftValue...)
	res = append(res, rightValue...)

	fields, _ := s.Fields()

	return rows.NewBinaryVirtualRow(fields, res)
}

func (s *SortMergeJoinDataSet) equalCompare(outerRow proto.Row, innerRow proto.Row, outerValue proto.Value) (proto.Row, error) {
	if err := s.updateLastRow(outerRow, outerValue); err != nil {
		return nil, err
	}

	if !s.DescartesFlag() {
		s.SetEqualValue(cast.ToString(outerValue), innerRow)
	}

	return s.resGenerate(outerRow, innerRow), nil
}

func (s *SortMergeJoinDataSet) updateLastRow(outerRow proto.Row, outerValue proto.Value) error {
	s.lastRow = outerRow
	if s.DescartesFlag() {
		index := s.EqualIndex(outerValue.String())
		if index == 0 {
			nextOuterRow, err := s.getOuterRow()
			if err != nil {
				return err
			}
			s.lastRow = nextOuterRow
		}
	}

	return nil
}

func (s *SortMergeJoinDataSet) greaterCompare(outerRow proto.Row) (proto.Row, proto.Row, error) {
	s.lastRow = nil
	// if leftDs row equal last row, do descartes match
	outerValue, err := outerRow.(proto.KeyedRow).Get(s.joinColumn.Column)
	if err != nil {
		return nil, nil, err
	}

	if outerRow == nil {
		return nil, nil, nil
	}

	var innerRow proto.Row
	if s.isDescartes(outerValue.String()) {
		s.setDescartesFlag(IsDescartes)
		innerRow = s.EqualValue(outerValue.String())
	} else {
		s.setDescartesFlag(NotDescartes)
		innerRow, err = s.getInnerRow(outerRow)
		if err != nil {
			return nil, nil, err
		}
	}

	return outerRow, innerRow, nil
}
