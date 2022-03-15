// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.impl.numericbuilder.ArrayTypeArrowUInt8Type;
import io.v6d.arrow.impl.numericbuilder.ValueTypeArrowUInt8Type;
import io.v6d.std.CBool;
import io.v6d.std.CUnsignedChar;
import io.v6d.std.shared_ptr;
import io.v6d.std.vector;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::UInt8Builder")
@FFIGen
@CXXHead(
        system = "arrow/array/builder_primitive.h"
)
public interface UInt8Builder extends CXXPointer {
    @FFIExpr("{0}")
    NumericBuilder<UInt8Type> get();

    @CXXValue
    Status Append(@FFIConst char val);

    @CXXValue
    Status AppendNulls(long length);

    @CXXValue
    Status AppendNull();

    @CXXValue
    Status AppendEmptyValue();

    @CXXValue
    Status AppendEmptyValues(long length);

    char GetValue(long index);

    void Reset();

    @CXXValue
    Status Resize(long capacity);

    @CXXValue
    Status AppendValues(ValueTypeArrowUInt8Type values, long length, CUnsignedChar valid_bytes);

    @CXXValue
    Status AppendValues(ValueTypeArrowUInt8Type values, long length, CUnsignedChar bitmap,
            long bitmap_offset);

    @CXXValue
    Status AppendValues(ValueTypeArrowUInt8Type values, long length,
            @CXXReference @FFITypeAlias("const std::vector<bool>") vector<CBool> is_valid);

    @CXXValue
    Status AppendValues(
            @CXXReference @FFITypeAlias("const std::vector<arrow::NumericBuilder<arrow::UInt8Type>::value_type>") vector<ValueTypeArrowUInt8Type> values,
            @CXXReference @FFITypeAlias("const std::vector<bool>") vector<CBool> is_valid);

    @CXXValue
    Status AppendValues(
            @CXXReference @FFITypeAlias("const std::vector<arrow::NumericBuilder<arrow::UInt8Type>::value_type>") vector<ValueTypeArrowUInt8Type> values);

    @CXXValue
    Status FinishInternal(
            @FFITypeAlias("std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> out);

    @CXXValue
    Status Finish(
            @FFITypeAlias("std::shared_ptr<arrow::NumericBuilder<arrow::UInt8Type>::ArrayType>") shared_ptr<ArrayTypeArrowUInt8Type> out);

    @CXXValue
    Status AppendArraySlice(@CXXReference ArrayData array, long offset, long length);

    void UnsafeAppend(@FFIConst char val);

    void UnsafeAppendNull();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::DataType>")
    shared_ptr<DataType> type();

    static UInt8Builder cast(final long __foreign_address) {
        try {
            Class<UInt8Builder> clz = (Class<UInt8Builder>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(UInt8Builder.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static UInt8Builder cast(final FFIPointer __foreign_pointer) {
        return UInt8Builder.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(UInt8Builder.class, true));
    }

    static UInt8Builder create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::DataType>") shared_ptr<DataType> type,
            MemoryPool pool) {
        return UInt8Builder.getFactory().create(type, pool);
    }

    @FFIFactory
    @CXXHead(
            system = "arrow/array/builder_primitive.h"
    )
    interface Factory {
        UInt8Builder create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::DataType>") shared_ptr<DataType> type,
                MemoryPool pool);
    }
}
