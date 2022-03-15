// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard.impl.numericarraybuilder;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.ArrayData;
import io.v6d.arrow.Buffer;
import io.v6d.arrow.DataType;
import io.v6d.arrow.impl.numericarray.IteratorTypeArrowFloatType;
import io.v6d.arrow.impl.numericarray.ValueTypeArrowFloatType;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("vineyard::NumericArrayBuilder<float>::ArrayType")
@FFIGen
@CXXHead("basic/ds/arrow.h")
public interface ArrayTypeFloat extends CXXPointer {
    @FFIExpr("{0}")
    io.v6d.vineyard.impl.converttoarrowtype.ArrayTypeFloat get();

    ValueTypeArrowFloatType raw_values();

    float Value(long i);

    float GetView(long i);

    @CXXValue
    IteratorTypeArrowFloatType begin();

    @CXXValue
    IteratorTypeArrowFloatType end();

    static ArrayTypeFloat cast(final long __foreign_address) {
        try {
            Class<ArrayTypeFloat> clz = (Class<ArrayTypeFloat>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ArrayTypeFloat.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ArrayTypeFloat cast(final FFIPointer __foreign_pointer) {
        return ArrayTypeFloat.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(ArrayTypeFloat.class, true));
    }

    static ArrayTypeFloat create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data) {
        return ArrayTypeFloat.getFactory().create(data);
    }

    static ArrayTypeFloat create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::DataType>") shared_ptr<DataType> arg0,
            long arg1,
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> arg2,
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> arg3,
            long arg4, long arg5) {
        return ArrayTypeFloat.getFactory().create(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @FFIFactory
    @CXXHead("basic/ds/arrow.h")
    interface Factory {
        ArrayTypeFloat create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data);

        ArrayTypeFloat create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::DataType>") shared_ptr<DataType> arg0,
                long arg1,
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> arg2,
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> arg3,
                long arg4, long arg5);
    }
}
