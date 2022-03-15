// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.CXXEnumMap;
import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.FFITypeRefiner;
import io.v6d.std.CInt;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::DateUnit")
@FFITypeRefiner("io.v6d.arrow.DateUnit.get")
@CXXHead(
        system = "arrow/type_fwd.h"
)
public enum DateUnit implements CXXEnum {
    DAY(Library.INSTANCE.DAY()),

    MILLI(Library.INSTANCE.MILLI());

    private static final CXXEnumMap<DateUnit> $map = new CXXEnumMap<>(values());

    int $value;

    DateUnit(int value) {
        $value = value;
    }

    DateUnit(CInt value) {
        $value = value.get();
    }

    public static DateUnit get(int value) {
        return $map.get(value);
    }

    public static DateUnit get(CInt value) {
        return $map.get(value.get());
    }

    public int getValue() {
        return $value;
    }

    public static DateUnit cast(final long __foreign_address) {
        try {
            Class<DateUnit> clz = (Class<DateUnit>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(DateUnit.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    public static DateUnit cast(final FFIPointer __foreign_pointer) {
        return DateUnit.cast(__foreign_pointer.getAddress());
    }

    @FFIGen
    @FFILibrary(
            value = "arrow::DateUnit",
            namespace = "arrow::DateUnit"
    )
    @CXXHead(
            system = "arrow/type_fwd.h"
    )
    public interface Library {
        Library INSTANCE = FFITypeFactory.getLibrary(Library.class);

        @FFIGetter
        int DAY();

        @FFIGetter
        int MILLI();
    }
}
