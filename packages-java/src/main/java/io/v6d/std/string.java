// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFIStringProvider;
import com.alibaba.fastffi.FFIStringReceiver;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.impl.CharPointer;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("std::string")
@FFIGen
@CXXHead(
        system = "iosfwd"
)
@CXXHead(
        system = "string"
)
public interface string extends CXXPointer, FFIStringProvider, FFIStringReceiver {
    long size();

    long data();

    static string cast(final long __foreign_address) {
        try {
            Class<string> clz = (Class<string>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(string.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static string cast(final FFIPointer __foreign_pointer) {
        return string.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(string.class, true));
    }

    static string create() {
        return string.getFactory().create();
    }

    static string create(CharPointer buffer) {
        return string.getFactory().create(buffer);
    }

    static string create(CharPointer buffer, long length) {
        return string.getFactory().create(buffer, length);
    }

    static string create(@CXXReference string other) {
        return string.getFactory().create(other);
    }

    static string create(String str) {
        return string.getFactory().create(str);
    }

    @FFIFactory
    interface Factory {
        string create();

        string create(CharPointer buffer);

        string create(CharPointer buffer, long length);

        string create(@CXXReference string other);

        default string create(String str) {
            string s = create();
            s.fromJavaString(str);
            return s;
        }
    }
}
