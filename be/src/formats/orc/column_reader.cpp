// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "formats/orc/column_reader.h"

#include "common/statusor.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/utils.h"

namespace starrocks {

StatusOr<std::unique_ptr<ORCColumnReader>> ORCColumnReader::create(const TypeDescriptor& type,
                                                                   const orc::Type* orc_type, bool nullable,
                                                                   const OrcMappingPtr& orc_mapping,
                                                                   OrcChunkReader* reader) {
    if (type.is_complex_type() && orc_mapping == nullptr) {
        return Status::InternalError("Complex type must having OrcMapping");
    }
    if (orc_type == nullptr) {
        return Status::InternalError("Each ColumnReader's ORC type must non-nullptr");
    }

    switch (type.type) {
    case TYPE_BOOLEAN:
        return std::make_unique<BooleanColumnReader>(type, orc_type, nullable, reader);
    case TYPE_TINYINT:
        return std::make_unique<IntColumnReader<TYPE_TINYINT>>(type, orc_type, nullable, reader);
    case TYPE_SMALLINT:
        return std::make_unique<IntColumnReader<TYPE_SMALLINT>>(type, orc_type, nullable, reader);
    case TYPE_INT:
        return std::make_unique<IntColumnReader<TYPE_INT>>(type, orc_type, nullable, reader);
    case TYPE_BIGINT:
        return std::make_unique<IntColumnReader<TYPE_BIGINT>>(type, orc_type, nullable, reader);
    case TYPE_LARGEINT:
        return std::make_unique<IntColumnReader<TYPE_LARGEINT>>(type, orc_type, nullable, reader);
    case TYPE_FLOAT:
        return std::make_unique<DoubleColumnReader<TYPE_FLOAT>>(type, orc_type, nullable, reader);
    case TYPE_DOUBLE:
        return std::make_unique<DoubleColumnReader<TYPE_DOUBLE>>(type, orc_type, nullable, reader);
    case TYPE_DECIMAL:
        return std::make_unique<DecimalColumnReader>(type, orc_type, nullable, reader);
    case TYPE_DECIMALV2:
        return std::make_unique<DecimalColumnReader>(type, orc_type, nullable, reader);
    case TYPE_DECIMAL32:
        return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL32>>(type, orc_type, nullable, reader);
    case TYPE_DECIMAL64:
        return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL64>>(type, orc_type, nullable, reader);
    case TYPE_DECIMAL128:
        return std::make_unique<Decimal32Or64Or128ColumnReader<TYPE_DECIMAL128>>(type, orc_type, nullable, reader);
    case TYPE_CHAR:
        return std::make_unique<StringColumnReader>(type, orc_type, nullable, reader);
    case TYPE_VARCHAR:
        return std::make_unique<StringColumnReader>(type, orc_type, nullable, reader);
    case TYPE_VARBINARY:
        return std::make_unique<VarbinaryColumnReader>(type, orc_type, nullable, reader);
    case TYPE_DATE:
        return std::make_unique<DateColumnReader>(type, orc_type, nullable, reader);
    case TYPE_DATETIME:
        if (orc_type->getKind() == orc::TypeKind::TIMESTAMP) {
            return std::make_unique<TimestampColumnReader<false>>(type, orc_type, nullable, reader);
        } else if (orc_type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT) {
            return std::make_unique<TimestampColumnReader<true>>(type, orc_type, nullable, reader);
        } else {
            return Status::InternalError("Failed to create column reader about TYPE_DATETIME");
        }
    case TYPE_TIME:
        return std::make_unique<TimeColumnReader>(type, orc_type, nullable, reader);
    case TYPE_STRUCT: {
        std::vector<std::unique_ptr<ORCColumnReader>> child_readers;
        for (size_t i = 0; i < type.children.size(); i++) {
            const TypeDescriptor& child_type = type.children[i];
            ASSIGN_OR_RETURN(
                    std::unique_ptr<ORCColumnReader> child_reader,
                    ORCColumnReader::create(child_type, orc_mapping->get_orc_type_child_mapping(i).orc_type, true,
                                            orc_mapping->get_orc_type_child_mapping(i).orc_mapping, reader));
            child_readers.emplace_back(std::move(child_reader));
        }
        return std::make_unique<StructColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
    }

    case TYPE_ARRAY: {
        std::vector<std::unique_ptr<ORCColumnReader>> child_readers{};
        const TypeDescriptor& child_type = type.children[0];
        ASSIGN_OR_RETURN(std::unique_ptr<ORCColumnReader> child_reader,
                         ORCColumnReader::create(child_type, orc_mapping->get_orc_type_child_mapping(0).orc_type, true,
                                                 orc_mapping->get_orc_type_child_mapping(0).orc_mapping, reader));
        child_readers.emplace_back(std::move(child_reader));
        return std::make_unique<ArrayColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
    }
    case TYPE_MAP: {
        std::vector<std::unique_ptr<ORCColumnReader>> child_readers{};
        const TypeDescriptor& key_type = type.children[0];
        const TypeDescriptor& value_type = type.children[1];
        if (key_type.is_unknown_type()) {
            child_readers.emplace_back(nullptr);
        } else {
            ASSIGN_OR_RETURN(
                    std::unique_ptr<ORCColumnReader> key_reader,
                    ORCColumnReader::create(key_type, orc_mapping->get_orc_type_child_mapping(0).orc_type, true,
                                            orc_mapping->get_orc_type_child_mapping(0).orc_mapping, reader));
            child_readers.emplace_back(std::move(key_reader));
        }

        if (value_type.is_unknown_type()) {
            child_readers.emplace_back(nullptr);
        } else {
            ASSIGN_OR_RETURN(
                    std::unique_ptr<ORCColumnReader> value_reader,
                    ORCColumnReader::create(value_type, orc_mapping->get_orc_type_child_mapping(1).orc_type, true,
                                            orc_mapping->get_orc_type_child_mapping(1).orc_mapping, reader));
            child_readers.emplace_back(std::move(value_reader));
        }
        return std::make_unique<MapColumnReader>(type, orc_type, nullable, reader, std::move(child_readers));
    }
    default:
        return Status::InternalError("Unsupported type");
    }
}

StatusOr<std::unique_ptr<ORCColumnReader>> ORCColumnReader::create_default_column_reader(const TypeDescriptor& type,
                                                                                         bool nullable,
                                                                                         OrcChunkReader* reader) {
    return std::make_unique<DefaultColumnReader>(type, nullable, reader);
}

Status DefaultColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    col->append_default(size);
    return Status::OK();
}

Status BooleanColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& column, size_t from, size_t size) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);
    size_t column_start = column->size();
    column->resize_uninitialized(column_start + size);
    if (_nullable) {
        auto null_column = ColumnHelper::as_raw_column<NullableColumn>(column);
        handle_null(cvb, null_column, column_start, from, size);
    }

    Column* data_column = ColumnHelper::get_data_column(column.get());
    uint8_t* values = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(data_column)->get_data().data();
    int64_t* cvb_data = data->data.data();

    for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size; column_pos++, vb_pos++) {
        values[column_pos] = (cvb_data[vb_pos] != 0);
    }
    return Status::OK();
}

template <LogicalType Type>
Status IntColumnReader<Type>::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    if (_nullable) {
        {
            auto* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
            if (data != nullptr) {
                return _fill_int_column_with_null_from_cvb<orc::LongVectorBatch>(data, col, from, size);
            }
        }
        {
            auto* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
            if (data != nullptr) {
                return _fill_int_column_with_null_from_cvb<orc::DoubleVectorBatch>(data, col, from, size);
            }
        }
        {
            // we have nothing to fill, but have to resize column to save from crash.
            col->resize(col->size() + size);
        }
    } else {
        // try to dyn_cast to long vector batch first. in most case, the cast will succeed.
        // so there is no performance loss comparing to call `down_cast` directly
        // because in `down_cast` implementation, there is also a dyn_cast.
        {
            auto* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
            if (data != nullptr) {
                return _fill_int_column_from_cvb<orc::LongVectorBatch>(data, col, from, size);
            }
        }
        // if dyn_cast to long vector batch failed, try to dyn_cast to double vector batch for best effort
        // it only happens when slot type and orc type don't match.
        {
            auto* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
            if (data != nullptr) {
                return _fill_int_column_from_cvb<orc::DoubleVectorBatch>(data, col, from, size);
            }
        }
        {
            // we have nothing to fill, but have to resize column to save from crash.
            col->resize(col->size() + size);
        }
    }
    return Status::OK();
}

template <LogicalType Type>
template <typename OrcColumnVectorBatch>
Status IntColumnReader<Type>::_fill_int_column_with_null_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col,
                                                                  size_t from, size_t size) {
    size_t col_start = col->size();
    col->resize_uninitialized(col->size() + size);

    auto null_column = ColumnHelper::as_raw_column<NullableColumn>(col);
    auto nulls = null_column->null_column()->get_data().data();
    handle_null(data, null_column, col_start, from, size, false);

    auto* values = ColumnHelper::cast_to_raw<Type>(null_column->data_column())->get_data().data();
    auto* cvbd = data->data.data();

    for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
        values[i] = cvbd[cvb_pos];
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    if (_reader->get_broker_load_mode() && from == 0 && col_start == 0) {
        auto filter = _reader->get_broker_load_fiter()->data();
        auto strict_mode = _reader->get_strict_mode();
        bool reported = false;

        if (strict_mode) {
            for (size_t i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (nulls[i] == 0 && (value < numeric_limits<RunTimeCppType<Type>>::lowest() ||
                                      value > numeric_limits<RunTimeCppType<Type>>::max())) {
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        auto slot = _reader->get_current_slot();
                        std::string error_msg = strings::Substitute(
                                "Value '$0' is out of range. The type of '$1' is $2'", std::to_string(value),
                                slot->col_name(), slot->type().debug_string());
                        _reader->report_error_message(error_msg);
                    }
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (nulls[i] == 0 && (value < numeric_limits<RunTimeCppType<Type>>::lowest() ||
                                      value > numeric_limits<RunTimeCppType<Type>>::max())) {
                    nulls[i] = 1;
                }
            }
        }
    }

    null_column->update_has_null();
    return Status::OK();
}

template <LogicalType Type>
template <typename OrcColumnVectorBatch>
Status IntColumnReader<Type>::_fill_int_column_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col, size_t from,
                                                        size_t size) {
    size_t col_start = col->size();
    col->resize_uninitialized(col_start + size);

    auto* values = ColumnHelper::cast_to_raw<Type>(col)->get_data().data();

    auto* cvbd = data->data.data();

    for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
        values[i] = cvbd[cvb_pos];
    }

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    constexpr bool wild_type = (Type == TYPE_BIGINT || Type == TYPE_LARGEINT);
    // don't do overflow check on BIGINT(int64_t) or LARGEINT(int128_t)
    if constexpr (!wild_type) {
        if (_reader->get_broker_load_mode() && from == 0 && col_start == 0) {
            auto filter = _reader->get_broker_load_fiter()->data();
            bool reported = false;
            for (size_t i = 0; i < size; i++) {
                int64_t value = cvbd[i];
                // overflow.
                if (value < numeric_limits<RunTimeCppType<Type>>::lowest() ||
                    value > numeric_limits<RunTimeCppType<Type>>::max()) {
                    // can not accept null, so we have to discard it.
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        auto slot = _reader->get_current_slot();
                        std::string error_msg = strings::Substitute(
                                "Value '$0' is out of range. The type of '$1' is $2'", std::to_string(value),
                                slot->col_name(), slot->type().debug_string());
                        _reader->report_error_message(error_msg);
                    }
                }
            }
        }
    }
    return Status::OK();
}

template <LogicalType Type>
Status DoubleColumnReader<Type>::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& column, size_t from, size_t size) {
    size_t column_start = column->size();
    column->resize_uninitialized(column_start + size);
    if (_nullable) {
        auto null_column = ColumnHelper::as_raw_column<NullableColumn>(column);
        handle_null(cvb, null_column, column_start, from, size);
    }

    {
        auto* data = dynamic_cast<orc::DoubleVectorBatch*>(cvb);
        if (data != nullptr) {
            return _fill_double_column_from_cvb(data, column, from, column_start, size);
        }
    }
    {
        auto* data = dynamic_cast<orc::LongVectorBatch*>(cvb);
        if (data != nullptr) {
            return _fill_double_column_from_cvb(data, column, from, column_start, size);
        }
    }
    return Status::InternalError("Unreachable code in DoubleColumnReader");
}

template <LogicalType Type>
template <typename OrcColumnVectorBatch>
Status DoubleColumnReader<Type>::_fill_double_column_from_cvb(OrcColumnVectorBatch* data, ColumnPtr& col,
                                                              const size_t vb_pos_from, const size_t column_start,
                                                              const size_t size) {
    Column* data_column = ColumnHelper::get_data_column(col.get());
    auto* values = ColumnHelper::cast_to_raw<Type>(data_column)->get_data().data();
    auto* cvb_data = data->data.data();

    for (size_t column_pos = column_start, vb_pos = vb_pos_from; column_pos < column_start + size;
         column_pos++, vb_pos++) {
        values[column_pos] = cvb_data[vb_pos];
    }
    return Status::OK();
}

Status DecimalColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    size_t col_start = col->size();
    col->resize_uninitialized(col_start + size);

    if (_nullable) {
        auto c = ColumnHelper::as_raw_column<NullableColumn>(col);
        handle_null(cvb, c, col_start, from, size);
    }

    Column* data_column = ColumnHelper::get_data_column(col.get());

    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        _fill_decimal_column_from_orc_decimal64(down_cast<orc::Decimal64VectorBatch*>(cvb), data_column, col_start,
                                                from, size);
    } else {
        _fill_decimal_column_from_orc_decimal128(down_cast<orc::Decimal128VectorBatch*>(cvb), data_column, col_start,
                                                 from, size);
    }

    return Status::OK();
}

void DecimalColumnReader::_fill_decimal_column_from_orc_decimal64(orc::Decimal64VectorBatch* cvb, Column* col,
                                                                  size_t col_start, size_t from, size_t size) {
    static_assert(sizeof(DecimalV2Value) == sizeof(int128_t));
    auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(col)->get_data().data());

    auto* cvbd = cvb->values.data();

    for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
        values[i] = static_cast<int128_t>(cvbd[cvb_pos]);
    }

    if (DecimalV2Value::SCALE < cvb->scale) {
        int128_t d = DecimalV2Value::get_scale_base(cvb->scale - DecimalV2Value::SCALE);
        for (size_t i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (DecimalV2Value::SCALE > cvb->scale) {
        int128_t m = DecimalV2Value::get_scale_base(DecimalV2Value::SCALE - cvb->scale);
        for (size_t i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}

void DecimalColumnReader::_fill_decimal_column_from_orc_decimal128(orc::Decimal128VectorBatch* cvb, Column* col,
                                                                   size_t col_start, size_t from, size_t size) {
    auto* values = reinterpret_cast<int128_t*>(down_cast<DecimalColumn*>(col)->get_data().data());

    for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
        uint64_t hi = cvb->values[cvb_pos].getHighBits();
        uint64_t lo = cvb->values[cvb_pos].getLowBits();
        values[i] = (((int128_t)hi) << 64) | (int128_t)lo;
    }
    if (DecimalV2Value::SCALE < cvb->scale) {
        int128_t d = DecimalV2Value::get_scale_base(cvb->scale - DecimalV2Value::SCALE);
        for (size_t i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] / d;
        }
    } else if (DecimalV2Value::SCALE > cvb->scale) {
        int128_t m = DecimalV2Value::get_scale_base(DecimalV2Value::SCALE - cvb->scale);
        for (size_t i = col_start; i < col_start + size; ++i) {
            values[i] = values[i] * m;
        }
    }
}

template <LogicalType DecimalType>
Status Decimal32Or64Or128ColumnReader<DecimalType>::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from,
                                                             size_t size) {
    if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
        _fill_decimal_column_generic<int64_t>(cvb, col, from, size);
    } else {
        _fill_decimal_column_generic<int128_t>(cvb, col, from, size);
    }
    return Status::OK();
}

template <LogicalType DecimalType>
template <typename T>
inline void Decimal32Or64Or128ColumnReader<DecimalType>::_fill_decimal_column_generic(orc::ColumnVectorBatch* cvb,
                                                                                      ColumnPtr& col, size_t from,
                                                                                      size_t size) {
    static_assert(is_decimal_column<RunTimeColumnType<DecimalType>>,
                  "Only support TYPE_DECIMAL32|TYPE_DECIMAL64|TYPE_DECIMAL128");

    using OrcDecimalBatchType = typename DecimalVectorBatchSelector<T>::Type;
    using ColumnType = RunTimeColumnType<DecimalType>;
    using Type = RunTimeCppType<DecimalType>;

    auto data = (OrcDecimalBatchType*)cvb;
    size_t col_start = col->size();
    col->resize_uninitialized(col->size() + size);

    ColumnType* decimal_column = nullptr;
    if (_nullable) {
        auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(col);
        handle_null(cvb, nullable_column, col_start, from, size);
        decimal_column = ColumnHelper::cast_to_raw<DecimalType>(nullable_column->data_column());
    } else {
        decimal_column = ColumnHelper::cast_to_raw<DecimalType>(col);
    }

    auto values = decimal_column->get_data().data();
    if (data->scale == decimal_column->scale()) {
        auto src_idx = from;
        for (size_t dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal_trivial<T, Type, false>(original_value, &values[dst_idx]);
        }
    } else if (data->scale > decimal_column->scale()) {
        auto scale_factor = get_scale_factor<T>(data->scale - decimal_column->scale());
        auto src_idx = from;
        for (int dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal<T, Type, T, false, false>(original_value, scale_factor, &values[dst_idx]);
        }
    } else {
        auto scale_factor = get_scale_factor<Type>(decimal_column->scale() - data->scale);
        auto src_idx = from;
        for (size_t dst_idx = col_start; dst_idx < col_start + size; ++dst_idx, ++src_idx) {
            T original_value;
            if constexpr (std::is_same_v<T, int128_t>) {
                original_value = ((int128_t)data->values[src_idx].getHighBits()) << 64;
                original_value |= (int128_t)data->values[src_idx].getLowBits();
            } else {
                original_value = data->values[src_idx];
            }
            DecimalV3Cast::to_decimal<T, Type, Type, true, false>(original_value, scale_factor, &values[dst_idx]);
        }
    }
}

Status StringColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    auto* data = down_cast<orc::StringVectorBatch*>(cvb);

    size_t len = 0;
    for (size_t i = 0; i < size; ++i) {
        len += data->length[from + i];
    }
    size_t col_start = col->size();

    if (_nullable) {
        auto* c = ColumnHelper::as_raw_column<NullableColumn>(col);
        c->null_column()->resize_uninitialized(col->size() + size);
        handle_null(cvb, c, col_start, from, size, false);
    }

    auto* values = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(ColumnHelper::get_data_column(col.get()));

    auto& vb = values->get_bytes();
    // Need to resize after insert, because of padding char existed
    vb.reserve(vb.size() + len);

    auto& vo = values->get_offset();
    // We can resize directly
    raw::stl_vector_resize_uninitialized(&vo, vo.size() + size);

    size_t write_pos = vb.size();
    if (cvb->hasNulls) {
        if (_type.type == TYPE_CHAR) {
            // Possibly there are some zero padding characters in value, we have to strip them off.
            for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
                if (cvb->notNull[cvb_pos]) {
                    size_t str_size = remove_trailing_spaces(data->data[cvb_pos], data->length[cvb_pos]);
                    strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], str_size);
                    write_pos += str_size;
                    // Need plus 1 for offset
                    vo[i + 1] = write_pos;
                } else {
                    // Need plus 1 for offset
                    vo[i + 1] = write_pos;
                }
            }
        } else {
            for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
                if (cvb->notNull[cvb_pos]) {
                    strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], data->length[cvb_pos]);
                    write_pos += data->length[cvb_pos];
                    // Need plus 1 for offset
                    vo[i + 1] = write_pos;
                } else {
                    // Need plus 1 for offset
                    vo[i + 1] = write_pos;
                }
            }
        }
    } else {
        if (_type.type == TYPE_CHAR) {
            // Possibly there are some zero padding characters in value, we have to strip them off.
            for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
                size_t str_size = remove_trailing_spaces(data->data[cvb_pos], data->length[cvb_pos]);
                strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], str_size);
                write_pos += str_size;
                // Need plus 1 for offset
                vo[i + 1] = write_pos;
            }
        } else {
            for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
                strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], data->length[cvb_pos]);
                write_pos += data->length[cvb_pos];
                // Need plus 1 for offset
                vo[i + 1] = write_pos;
            }
        }
    }

    vb.resize(write_pos);

    // col_start == 0 and from == 0 means it's at top level of fill chunk, not in the middle of array
    // otherwise `broker_load_filter` does not work.
    if (_reader->get_broker_load_mode() && from == 0 && col_start == 0) {
        auto* filter = _reader->get_broker_load_fiter()->data();
        // only report once.
        bool reported = false;
        if (_nullable) {
            auto nulls = ColumnHelper::as_raw_column<NullableColumn>(col)->null_column()->get_data().data();
            auto strict_mode = _reader->get_strict_mode();
            if (strict_mode) {
                for (int i = 0; i < size; i++) {
                    // overflow.
                    if (nulls[i] == 0 && _type.len > 0 && data->length[i] > _type.len) {
                        filter[i] = 0;
                        if (!reported) {
                            reported = true;
                            std::string raw_data(data->data[i], data->length[i]);
                            auto slot = _reader->get_current_slot();
                            std::string error_msg =
                                    strings::Substitute("String '$0' is too long. The type of '$1' is $2'", raw_data,
                                                        slot->col_name(), slot->type().debug_string());
                            _reader->report_error_message(error_msg);
                        }
                    }
                }
            } else {
                for (int i = 0; i < size; i++) {
                    // overflow.
                    if (nulls[i] == 0 && _type.len > 0 && data->length[i] > _type.len) {
                        nulls[i] = 1;
                    }
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                // overflow.
                if (_type.len > 0 && data->length[i] > _type.len) {
                    // can not accept null, so we have to discard it.
                    filter[i] = 0;
                    if (!reported) {
                        reported = true;
                        std::string raw_data(data->data[i], data->length[i]);
                        auto slot = _reader->get_current_slot();
                        std::string error_msg =
                                strings::Substitute("String '$0' is too long. The type of '$1' is $2'", raw_data,
                                                    slot->col_name(), slot->type().debug_string());
                        _reader->report_error_message(error_msg);
                    }
                }
            }
        }
    }

    // Need to update_has_null, the broker load will change nulls
    if (_nullable) {
        auto* c = ColumnHelper::as_raw_column<NullableColumn>(col);
        c->update_has_null();
    }

    return Status::OK();
}

Status VarbinaryColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    auto* data = down_cast<orc::StringVectorBatch*>(cvb);
    size_t len = 0;
    for (size_t i = 0; i < size; ++i) {
        len += data->length[from + i];
    }
    size_t col_start = col->size();

    if (_nullable) {
        auto* c = ColumnHelper::as_raw_column<NullableColumn>(col);
        c->null_column()->resize_uninitialized(col->size() + size);
        handle_null(cvb, c, col_start, from, size);
    }

    auto* values = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(ColumnHelper::get_data_column(col.get()));
    auto& vb = values->get_bytes();
    auto& vo = values->get_offset();

    size_t write_pos = vb.size();

    // vb is using RawVectorPad16, resize will not initialize vector
    vb.resize(vb.size() + len);
    raw::stl_vector_resize_uninitialized(&vo, vo.size() + size);

    if (cvb->hasNulls) {
        for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
            if (cvb->notNull[cvb_pos]) {
                strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], data->length[cvb_pos]);
                write_pos += data->length[cvb_pos];
                // Plus 1 for offset
                vo[i + 1] = write_pos;
            } else {
                // Plus 1 for offset
                vo[i + 1] = write_pos;
            }
        }
    } else {
        for (size_t i = col_start, cvb_pos = from; i < col_start + size; ++i, ++cvb_pos) {
            strings::memcpy_inlined(&vb[write_pos], data->data[cvb_pos], data->length[cvb_pos]);
            write_pos += data->length[cvb_pos];
            // Plus 1 for offset
            vo[i + 1] = write_pos;
        }
    }
    return Status::OK();
}

Status DateColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& column, size_t from, size_t size) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);
    size_t column_start = column->size();
    column->resize_uninitialized(column_start + size);
    if (_nullable) {
        handle_null(cvb, ColumnHelper::as_raw_column<NullableColumn>(column), column_start, from, size);
    }
    Column* data_column = ColumnHelper::get_data_column(column.get());
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATE>(data_column)->get_data().data();

    if (cvb->hasNulls) {
        for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size;
             column_pos++, vb_pos++) {
            if (!cvb->notNull[column_pos]) {
                continue;
            }
            OrcDateHelper::orc_date_to_native_date(&(values[column_pos]), data->data[vb_pos]);
        }
    } else {
        for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size;
             column_pos++, vb_pos++) {
            OrcDateHelper::orc_date_to_native_date(&(values[column_pos]), data->data[vb_pos]);
        }
    }
    return Status::OK();
}

template <bool IsInstant>
Status TimestampColumnReader<IsInstant>::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& column, size_t from,
                                                  size_t size) {
    auto* data = down_cast<orc::TimestampVectorBatch*>(cvb);
    size_t column_start = column->size();
    column->resize_uninitialized(column_start + size);
    if (_nullable) {
        handle_null(cvb, ColumnHelper::as_raw_column<NullableColumn>(column), column_start, from, size);
    }
    Column* data_column = ColumnHelper::get_data_column(column.get());
    auto* values = ColumnHelper::cast_to_raw<TYPE_DATETIME>(data_column)->get_data().data();
    bool use_ns = _reader->use_nanoseconds_in_datetime();

    if (cvb->hasNulls) {
        for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size;
             column_pos++, vb_pos++) {
            if (!cvb->notNull[column_pos]) {
                continue;
            }
            int64_t ns = 0;
            if (use_ns) {
                ns = data->nanoseconds[vb_pos];
            }
            OrcTimestampHelper::orc_ts_to_native_ts(&(values[column_pos]), _reader->tzinfo(),
                                                    _reader->tzoffset_in_seconds(), data->data[vb_pos], ns, IsInstant);
        }
    } else {
        for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size;
             column_pos++, vb_pos++) {
            int64_t ns = 0;
            if (use_ns) {
                ns = data->nanoseconds[vb_pos];
            }
            OrcTimestampHelper::orc_ts_to_native_ts(&(values[column_pos]), _reader->tzinfo(),
                                                    _reader->tzoffset_in_seconds(), data->data[vb_pos], ns, IsInstant);
        }
    }
    return Status::OK();
}

Status TimeColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& column, size_t from, size_t size) {
    auto* data = down_cast<orc::LongVectorBatch*>(cvb);
    size_t column_start = column->size();
    column->resize_uninitialized(column_start + size);
    if (_nullable) {
        auto null_column = ColumnHelper::as_raw_column<NullableColumn>(column);
        handle_null(cvb, null_column, column_start, from, size);
    }

    Column* data_column = ColumnHelper::get_data_column(column.get());
    auto* values = ColumnHelper::cast_to_raw<TYPE_TIME>(data_column)->get_data().data();
    auto* cvb_data = data->data.data();
    for (size_t column_pos = column_start, vb_pos = from; column_pos < column_start + size; column_pos++, vb_pos++) {
        values[column_pos] = cvb_data[vb_pos] / 1000000;
    }

    return Status::OK();
}

Status ArrayColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    if (_nullable) {
        auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
        auto* col_nullable = down_cast<NullableColumn*>(col.get());
        auto* col_array = down_cast<ArrayColumn*>(col_nullable->data_column().get());

        if (!orc_list->hasNulls) {
            RETURN_IF_ERROR(_fill_array_column(orc_list, col_nullable->data_column(), from, size));
            col_nullable->null_column()->resize(col_array->size());
            return Status::OK();
        }
        // else

        const int end = from + size;

        int i = from;
        while (i < end) {
            int j = i;
            // Loop until NULL or end of batch.
            while (j < end && orc_list->notNull[j]) {
                j++;
            }
            if (j > i) {
                RETURN_IF_ERROR(_fill_array_column(orc_list, col_nullable->data_column(), i, j - i));
                col_nullable->null_column()->resize(col_array->size());
            }

            if (j == end) {
                break;
            }
            DCHECK(!orc_list->notNull[j]);
            i = j++;
            // Loop until not NULL or end of batch.
            while (j < end && !orc_list->notNull[j]) {
                j++;
            }
            col_nullable->append_nulls(j - i);
            i = j;
        }
    } else {
        RETURN_IF_ERROR(_fill_array_column(cvb, col, from, size));
    }
    return Status::OK();
}

Status ArrayColumnReader::_fill_array_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
    auto* col_array = down_cast<ArrayColumn*>(col.get());

    UInt32Column* offsets = col_array->offsets_column().get();
    copy_array_offset(orc_list->offsets, from, size + 1, offsets);

    ColumnPtr& elements = col_array->elements_column();
    const int elements_from = implicit_cast<int>(orc_list->offsets[from]);
    const int elements_size = implicit_cast<int>(orc_list->offsets[from + size] - elements_from);

    return _child_readers[0]->get_next(orc_list->elements.get(), elements, elements_from, elements_size);
}

Status MapColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    if (_nullable) {
        auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
        auto* col_nullable = down_cast<NullableColumn*>(col.get());
        auto* col_map = down_cast<MapColumn*>(col_nullable->data_column().get());

        if (!orc_map->hasNulls) {
            RETURN_IF_ERROR(_fill_map_column(orc_map, col_nullable->data_column(), from, size));
            col_nullable->null_column()->resize(col_map->size());
            return Status::OK();
        }
        // else

        const int end = from + size;

        int i = from;
        while (i < end) {
            int j = i;
            // Loop until NULL or end of batch.
            while (j < end && orc_map->notNull[j]) {
                j++;
            }
            if (j > i) {
                RETURN_IF_ERROR(_fill_map_column(orc_map, col_nullable->data_column(), i, j - i));
                col_nullable->null_column()->resize(col_map->size());
            }

            if (j == end) {
                break;
            }
            DCHECK(!orc_map->notNull[j]);
            i = j++;
            // Loop until not NULL or end of batch.
            while (j < end && !orc_map->notNull[j]) {
                j++;
            }
            col_nullable->append_nulls(j - i);
            i = j;
        }
    } else {
        RETURN_IF_ERROR(_fill_map_column(cvb, col, from, size));
    }
    return Status::OK();
}

Status MapColumnReader::_fill_map_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
    auto* col_map = down_cast<MapColumn*>(col.get());

    UInt32Column* offsets = col_map->offsets_column().get();
    copy_array_offset(orc_map->offsets, from, size + 1, offsets);

    ColumnPtr& keys = col_map->keys_column();
    const int keys_from = implicit_cast<int>(orc_map->offsets[from]);
    const int keys_size = implicit_cast<int>(orc_map->offsets[from + size] - keys_from);

    if (_child_readers[0] == nullptr) {
        keys->append_default(keys_size);
    } else {
        RETURN_IF_ERROR(_child_readers[0]->get_next(orc_map->keys.get(), keys, keys_from, keys_size));
    }

    ColumnPtr& values = col_map->values_column();
    const int values_from = implicit_cast<int>(orc_map->offsets[from]);
    const int values_size = implicit_cast<int>(orc_map->offsets[from + size] - values_from);
    if (_child_readers[1] == nullptr) {
        values->append_default(values_size);
    } else {
        RETURN_IF_ERROR(_child_readers[1]->get_next(orc_map->elements.get(), values, values_from, values_size));
    }
    return Status::OK();
}

Status StructColumnReader::get_next(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    if (_nullable) {
        auto* orc_struct = down_cast<orc::StructVectorBatch*>(cvb);
        auto* col_nullable = down_cast<NullableColumn*>(col.get());
        auto* col_struct = down_cast<StructColumn*>(col_nullable->data_column().get());

        if (!orc_struct->hasNulls) {
            RETURN_IF_ERROR(_fill_struct_column(cvb, col_nullable->data_column(), from, size));
            col_nullable->null_column()->resize(col_struct->size());
        } else {
            const int end = from + size;
            int i = from;

            while (i < end) {
                int j = i;
                // Loop until NULL or end of batch.
                while (j < end && orc_struct->notNull[j]) {
                    j++;
                }
                if (j > i) {
                    RETURN_IF_ERROR(_fill_struct_column(orc_struct, col_nullable->data_column(), i, j - i));
                    col_nullable->null_column()->resize(col_struct->size());
                }

                if (j == end) {
                    break;
                }
                DCHECK(!orc_struct->notNull[j]);
                i = j++;
                // Loop until not NULL or end of batch.
                while (j < end && !orc_struct->notNull[j]) {
                    j++;
                }
                col_nullable->append_nulls(j - i);
                i = j;
            }
        }
    } else {
        RETURN_IF_ERROR(_fill_struct_column(cvb, col, from, size));
    }
    return Status::OK();
}

Status StructColumnReader::_fill_struct_column(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size) {
    auto* orc_struct = down_cast<orc::StructVectorBatch*>(cvb);
    auto* col_struct = down_cast<StructColumn*>(col.get());

    auto& field_columns = col_struct->fields_column();

    for (size_t i = 0; i < _type.children.size(); i++) {
        size_t column_id = _child_readers[i]->get_orc_type()->getColumnId();
        orc::ColumnVectorBatch* field_cvb = orc_struct->fieldsColumnIdMap[column_id];
        RETURN_IF_ERROR(_child_readers[i]->get_next(field_cvb, field_columns[i], from, size));
    }
    return Status::OK();
}

} // namespace starrocks
