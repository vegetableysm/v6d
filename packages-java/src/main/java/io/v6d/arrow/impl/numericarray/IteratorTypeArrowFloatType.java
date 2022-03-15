// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow.impl.numericarray;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.FloatType;
import io.v6d.arrow.NumericArray;
import io.v6d.arrow.stl.ArrayIterator;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::NumericArray<arrow::FloatType>::IteratorType")
@FFIGen
@CXXHead(
        system = "arrow/array/array_primitive.h"
)
public interface IteratorTypeArrowFloatType extends CXXPointer {
    @FFIExpr("{0}")
    ArrayIterator<NumericArray<FloatType>> get();

    static IteratorTypeArrowFloatType cast(final long __foreign_address) {
        try {
            Class<IteratorTypeArrowFloatType> clz = (Class<IteratorTypeArrowFloatType>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(IteratorTypeArrowFloatType.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static IteratorTypeArrowFloatType cast(final FFIPointer __foreign_pointer) {
        return IteratorTypeArrowFloatType.cast(__foreign_pointer.getAddress());
    }
}
