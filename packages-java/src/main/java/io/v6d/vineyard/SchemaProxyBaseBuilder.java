// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("vineyard::SchemaProxyBaseBuilder")
@FFIGen
@CXXHead("basic/ds/arrow.vineyard.h")
public interface SchemaProxyBaseBuilder extends ObjectBuilder, FFIPointer {
    @CXXValue
    @FFITypeAlias("std::shared_ptr<vineyard::Object>")
    shared_ptr<Object> _Seal(@CXXReference Client client);

    @CXXValue
    Status Build(@CXXReference Client client);

    static SchemaProxyBaseBuilder cast(final long __foreign_address) {
        try {
            Class<SchemaProxyBaseBuilder> clz = (Class<SchemaProxyBaseBuilder>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(SchemaProxyBaseBuilder.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static SchemaProxyBaseBuilder cast(final FFIPointer __foreign_pointer) {
        return SchemaProxyBaseBuilder.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(SchemaProxyBaseBuilder.class, true));
    }

    static SchemaProxyBaseBuilder create(@CXXReference Client client) {
        return SchemaProxyBaseBuilder.getFactory().create(client);
    }

    static SchemaProxyBaseBuilder create(@CXXReference SchemaProxy __value) {
        return SchemaProxyBaseBuilder.getFactory().create(__value);
    }

    static SchemaProxyBaseBuilder create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<vineyard::SchemaProxy>") shared_ptr<SchemaProxy> __value) {
        return SchemaProxyBaseBuilder.getFactory().create(__value);
    }

    @FFIFactory
    @CXXHead("basic/ds/arrow.vineyard.h")
    interface Factory {
        SchemaProxyBaseBuilder create(@CXXReference Client client);

        SchemaProxyBaseBuilder create(@CXXReference SchemaProxy __value);

        SchemaProxyBaseBuilder create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<vineyard::SchemaProxy>") shared_ptr<SchemaProxy> __value);
    }
}
