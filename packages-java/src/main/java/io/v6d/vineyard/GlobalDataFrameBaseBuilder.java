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

@FFITypeAlias("vineyard::GlobalDataFrameBaseBuilder")
@FFIGen
@CXXHead("basic/ds/dataframe.h")
public interface GlobalDataFrameBaseBuilder extends ObjectBuilder, FFIPointer {
    @CXXValue
    @FFITypeAlias("std::shared_ptr<vineyard::Object>")
    shared_ptr<Object> _Seal(@CXXReference Client client);

    @CXXValue
    Status Build(@CXXReference Client client);

    static GlobalDataFrameBaseBuilder cast(final long __foreign_address) {
        try {
            Class<GlobalDataFrameBaseBuilder> clz = (Class<GlobalDataFrameBaseBuilder>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(GlobalDataFrameBaseBuilder.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static GlobalDataFrameBaseBuilder cast(final FFIPointer __foreign_pointer) {
        return GlobalDataFrameBaseBuilder.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(GlobalDataFrameBaseBuilder.class, true));
    }

    static GlobalDataFrameBaseBuilder create(@CXXReference Client client) {
        return GlobalDataFrameBaseBuilder.getFactory().create(client);
    }

    static GlobalDataFrameBaseBuilder create(@CXXReference GlobalDataFrame __value) {
        return GlobalDataFrameBaseBuilder.getFactory().create(__value);
    }

    static GlobalDataFrameBaseBuilder create(
            @CXXReference @FFITypeAlias("const std::shared_ptr<vineyard::GlobalDataFrame>") shared_ptr<GlobalDataFrame> __value) {
        return GlobalDataFrameBaseBuilder.getFactory().create(__value);
    }

    @FFIFactory
    @CXXHead("basic/ds/dataframe.h")
    interface Factory {
        GlobalDataFrameBaseBuilder create(@CXXReference Client client);

        GlobalDataFrameBaseBuilder create(@CXXReference GlobalDataFrame __value);

        GlobalDataFrameBaseBuilder create(
                @CXXReference @FFITypeAlias("const std::shared_ptr<vineyard::GlobalDataFrame>") shared_ptr<GlobalDataFrame> __value);
    }
}
