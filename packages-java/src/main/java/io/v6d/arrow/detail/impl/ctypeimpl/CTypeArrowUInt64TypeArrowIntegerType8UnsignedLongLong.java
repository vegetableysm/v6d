// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow.detail.impl.ctypeimpl;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::detail::CTypeImpl<arrow::UInt64Type, arrow::IntegerType, 8, unsigned long long>::c_type")
@FFIGen
@CXXHead(
        system = "arrow/type.h"
)
public interface CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong extends CXXPointer {
    @FFIExpr("(*{0})")
    long get();

    @FFIExpr("*{0} = (arrow::detail::CTypeImpl<arrow::UInt64Type, arrow::IntegerType, 8, unsigned long long>::c_type){1}")
    void set(long __value);

    static CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong cast(
            final long __foreign_address) {
        try {
            Class<CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong> clz = (Class<CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong cast(
            final FFIPointer __foreign_pointer) {
        return CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong.class, true));
    }

    static CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong create() {
        return CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong.getFactory().create();
    }

    static CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong create(long __value) {
        return CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong.getFactory().create(__value);
    }

    @FFIFactory
    @CXXHead(
            system = "arrow/type.h"
    )
    interface Factory {
        CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong create();

        CTypeArrowUInt64TypeArrowIntegerType8UnsignedLongLong create(long __value);
    }
}
