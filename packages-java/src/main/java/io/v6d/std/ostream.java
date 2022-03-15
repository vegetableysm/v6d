// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIExpr;
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

@FFITypeAlias("std::ostream")
@FFIGen
@CXXHead(
        system = "iosfwd"
)
@CXXHead(
        system = "ostream"
)
public interface ostream extends CXXPointer {
    @FFIExpr("{0}")
    basic_ostream<CChar> get();

    static ostream cast(final long __foreign_address) {
        try {
            Class<ostream> clz = (Class<ostream>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ostream.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ostream cast(final FFIPointer __foreign_pointer) {
        return ostream.cast(__foreign_pointer.getAddress());
    }
}
