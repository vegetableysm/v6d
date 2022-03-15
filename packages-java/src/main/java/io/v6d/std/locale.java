// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
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

@FFITypeAlias("std::locale")
@FFIGen
@CXXHead(
        system = "__locale"
)
public interface locale extends CXXPointer {
    @CXXValue
    string name();

    static locale cast(final long __foreign_address) {
        try {
            Class<locale> clz = (Class<locale>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(locale.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static locale cast(final FFIPointer __foreign_pointer) {
        return locale.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(locale.class, true));
    }

    static locale create() {
        return locale.getFactory().create();
    }

    static locale create(@CXXReference locale arg0) {
        return locale.getFactory().create(arg0);
    }

    static locale create(CChar arg0) {
        return locale.getFactory().create(arg0);
    }

    static locale create(@CXXReference string arg0) {
        return locale.getFactory().create(arg0);
    }

    static locale create(@CXXReference locale arg0, CChar arg1, int arg2) {
        return locale.getFactory().create(arg0, arg1, arg2);
    }

    static locale create(@CXXReference locale arg0, @CXXReference string arg1, int arg2) {
        return locale.getFactory().create(arg0, arg1, arg2);
    }

    static locale create(@CXXReference locale arg0, @CXXReference locale arg1, int arg2) {
        return locale.getFactory().create(arg0, arg1, arg2);
    }

    @FFIFactory
    @CXXHead(
            system = "__locale"
    )
    interface Factory {
        locale create();

        locale create(@CXXReference locale arg0);

        locale create(CChar arg0);

        locale create(@CXXReference string arg0);

        locale create(@CXXReference locale arg0, CChar arg1, int arg2);

        locale create(@CXXReference locale arg0, @CXXReference string arg1, int arg2);

        locale create(@CXXReference locale arg0, @CXXReference locale arg1, int arg2);
    }

    @FFIGen
    @FFILibrary(
            value = "std::locale",
            namespace = "std::locale"
    )
    @CXXHead(
            system = "__locale"
    )
    interface Library {
        Library INSTANCE = FFITypeFactory.getLibrary(Library.class);

        @CXXValue
        locale global(@CXXReference locale arg0);

        @CXXReference
        locale classic();
    }

    @FFITypeAlias("std::locale::category")
    @FFIGen
    interface category extends CXXPointer {
        @FFIExpr("(*{0})")
        int get();

        @FFIExpr("*{0} = (std::locale::category){1}")
        void set(int __value);

        static category cast(final long __foreign_address) {
            try {
                Class<category> clz = (Class<category>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(category.class, true));
                return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
            } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
                return null;
            }
        }

        static category cast(final FFIPointer __foreign_pointer) {
            return category.cast(__foreign_pointer.getAddress());
        }

        static Factory getFactory() {
            return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(category.class, true));
        }

        static category create() {
            return category.getFactory().create();
        }

        static category create(int __value) {
            return category.getFactory().create(__value);
        }

        @FFIFactory
        interface Factory {
            category create();

            category create(int __value);
        }
    }
}
