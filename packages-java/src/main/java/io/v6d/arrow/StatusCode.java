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

@FFITypeAlias("arrow::StatusCode")
@FFITypeRefiner("io.v6d.arrow.StatusCode.get")
@CXXHead(
        system = "arrow/status.h"
)
public enum StatusCode implements CXXEnum {
    OK(Library.INSTANCE.OK()),

    OutOfMemory(Library.INSTANCE.OutOfMemory()),

    KeyError(Library.INSTANCE.KeyError()),

    TypeError(Library.INSTANCE.TypeError()),

    Invalid(Library.INSTANCE.Invalid()),

    IOError(Library.INSTANCE.IOError()),

    CapacityError(Library.INSTANCE.CapacityError()),

    IndexError(Library.INSTANCE.IndexError()),

    Cancelled(Library.INSTANCE.Cancelled()),

    UnknownError(Library.INSTANCE.UnknownError()),

    NotImplemented(Library.INSTANCE.NotImplemented()),

    SerializationError(Library.INSTANCE.SerializationError()),

    RError(Library.INSTANCE.RError()),

    CodeGenError(Library.INSTANCE.CodeGenError()),

    ExpressionValidationError(Library.INSTANCE.ExpressionValidationError()),

    ExecutionError(Library.INSTANCE.ExecutionError()),

    AlreadyExists(Library.INSTANCE.AlreadyExists());

    private static final CXXEnumMap<StatusCode> $map = new CXXEnumMap<>(values());

    int $value;

    StatusCode(int value) {
        $value = value;
    }

    StatusCode(CInt value) {
        $value = value.get();
    }

    public static StatusCode get(int value) {
        return $map.get(value);
    }

    public static StatusCode get(CInt value) {
        return $map.get(value.get());
    }

    public int getValue() {
        return $value;
    }

    public static StatusCode cast(final long __foreign_address) {
        try {
            Class<StatusCode> clz = (Class<StatusCode>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(StatusCode.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    public static StatusCode cast(final FFIPointer __foreign_pointer) {
        return StatusCode.cast(__foreign_pointer.getAddress());
    }

    @FFIGen
    @FFILibrary(
            value = "arrow::StatusCode",
            namespace = "arrow::StatusCode"
    )
    @CXXHead(
            system = "arrow/status.h"
    )
    public interface Library {
        Library INSTANCE = FFITypeFactory.getLibrary(Library.class);

        @FFIGetter
        int OK();

        @FFIGetter
        int OutOfMemory();

        @FFIGetter
        int KeyError();

        @FFIGetter
        int TypeError();

        @FFIGetter
        int Invalid();

        @FFIGetter
        int IOError();

        @FFIGetter
        int CapacityError();

        @FFIGetter
        int IndexError();

        @FFIGetter
        int Cancelled();

        @FFIGetter
        int UnknownError();

        @FFIGetter
        int NotImplemented();

        @FFIGetter
        int SerializationError();

        @FFIGetter
        int RError();

        @FFIGetter
        int CodeGenError();

        @FFIGetter
        int ExpressionValidationError();

        @FFIGetter
        int ExecutionError();

        @FFIGetter
        int AlreadyExists();
    }
}
