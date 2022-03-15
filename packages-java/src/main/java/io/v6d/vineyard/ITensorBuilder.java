// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
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

@FFITypeAlias("vineyard::ITensorBuilder")
@FFIGen
@CXXHead("basic/ds/tensor.h")
public interface ITensorBuilder extends CXXPointer {
    static ITensorBuilder cast(final long __foreign_address) {
        try {
            Class<ITensorBuilder> clz = (Class<ITensorBuilder>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ITensorBuilder.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ITensorBuilder cast(final FFIPointer __foreign_pointer) {
        return ITensorBuilder.cast(__foreign_pointer.getAddress());
    }
}
