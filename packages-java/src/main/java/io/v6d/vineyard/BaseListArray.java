// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.Array;
import io.v6d.std.CUnsignedChar;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("vineyard::BaseListArray")
@FFIGen
@CXXHead("basic/ds/arrow.vineyard.h")
public interface BaseListArray<ArrayType> extends ArrowArray, Registered<BaseListArray<ArrayType>>, FFIPointer {
    @FFINameAlias("Construct")
    void Construct_1(@CXXReference ObjectMeta meta);

    @FFINameAlias("PostConstruct")
    void PostConstruct_1(@CXXReference ObjectMeta meta);

    @CXXValue
    shared_ptr<ArrayType> GetArray();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Array>")
    shared_ptr<Array> ToArray();

    CUnsignedChar GetBase();

    static <ArrayType> BaseListArray<ArrayType> cast(Class<ArrayType> __arraytype,
            final long __foreign_address) {
        try {
            Class<BaseListArray<ArrayType>> clz = (Class<BaseListArray<ArrayType>>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(FFITypeFactory.makeParameterizedType(BaseListArray.class, __arraytype), true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static <ArrayType> BaseListArray<ArrayType> cast(Class<ArrayType> __arraytype,
            final FFIPointer __foreign_pointer) {
        return BaseListArray.cast(__arraytype, __foreign_pointer.getAddress());
    }
}
