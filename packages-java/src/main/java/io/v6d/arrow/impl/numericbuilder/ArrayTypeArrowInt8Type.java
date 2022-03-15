// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow.impl.numericbuilder;

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
import io.v6d.arrow.impl.numericarray.IteratorTypeArrowInt8Type;
import io.v6d.arrow.impl.numericarray.ValueTypeArrowInt8Type;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::NumericBuilder<arrow::Int8Type>::ArrayType")
@FFIGen
@CXXHead(
        system = "arrow/array/builder_primitive.h"
)
public interface ArrayTypeArrowInt8Type extends CXXPointer {
    @FFIExpr("{0}")
    io.v6d.arrow.impl.typetraits.ArrayTypeArrowInt8Type get();

    ValueTypeArrowInt8Type raw_values();

    byte Value(long i);

    byte GetView(long i);

    @CXXValue
    IteratorTypeArrowInt8Type begin();

    @CXXValue
    IteratorTypeArrowInt8Type end();

    static ArrayTypeArrowInt8Type cast(final long __foreign_address) {
        try {
            Class<ArrayTypeArrowInt8Type> clz = (Class<ArrayTypeArrowInt8Type>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ArrayTypeArrowInt8Type.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ArrayTypeArrowInt8Type cast(final FFIPointer __foreign_pointer) {
        return ArrayTypeArrowInt8Type.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(ArrayTypeArrowInt8Type.class, true));
    }

    static ArrayTypeArrowInt8Type create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data) {
        return ArrayTypeArrowInt8Type.getFactory().create(data);
    }

    @FFIFactory
    @CXXHead(
            system = "arrow/array/builder_primitive.h"
    )
    interface Factory {
        ArrayTypeArrowInt8Type create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::ArrayData>") shared_ptr<ArrayData> data);
    }
}
