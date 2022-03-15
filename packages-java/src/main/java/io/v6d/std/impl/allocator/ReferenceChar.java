// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std.impl.allocator;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.std.CChar;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("std::allocator<char>::reference")
@FFIGen
@CXXHead(
        system = "__memory/allocator.h"
)
public interface ReferenceChar extends CXXPointer {
    @FFIExpr("(*{0})")
    CChar get();

    @FFIExpr("*{0} = (std::allocator<char>::reference){1}")
    void set(CChar __value);

    static ReferenceChar cast(final long __foreign_address) {
        try {
            Class<ReferenceChar> clz = (Class<ReferenceChar>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ReferenceChar.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ReferenceChar cast(final FFIPointer __foreign_pointer) {
        return ReferenceChar.cast(__foreign_pointer.getAddress());
    }
}
