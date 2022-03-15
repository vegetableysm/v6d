// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard.impl.converttoarrowtype;

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
import io.v6d.arrow.TimestampArray;
import io.v6d.arrow.impl.numericarray.IteratorTypeArrowTimestampType;
import io.v6d.arrow.impl.numericarray.ValueTypeArrowTimestampType;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("vineyard::ConvertToArrowType<arrow::TimestampType>::ArrayType")
@FFIGen
@CXXHead("basic/ds/arrow_utils.h")
public interface ArrayTypeArrowTimestampType extends CXXPointer {
    @FFIExpr("{0}")
    TimestampArray get();

    ValueTypeArrowTimestampType raw_values();

    long Value(long i);

    long GetView(long i);

    @CXXValue
    IteratorTypeArrowTimestampType begin();

    @CXXValue
    IteratorTypeArrowTimestampType end();

    static ArrayTypeArrowTimestampType cast(final long __foreign_address) {
        try {
            Class<ArrayTypeArrowTimestampType> clz = (Class<ArrayTypeArrowTimestampType>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ArrayTypeArrowTimestampType.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ArrayTypeArrowTimestampType cast(final FFIPointer __foreign_pointer) {
        return ArrayTypeArrowTimestampType.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(ArrayTypeArrowTimestampType.class, true));
    }

    static ArrayTypeArrowTimestampType create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data) {
        return ArrayTypeArrowTimestampType.getFactory().create(data);
    }

    @FFIFactory
    @CXXHead("basic/ds/arrow_utils.h")
    interface Factory {
        ArrayTypeArrowTimestampType create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data);
    }
}
