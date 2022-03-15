// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std.impl.basic_string;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.std.impl.allocator_traits.PointerStdAllocatorChar;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("std::basic_string<char, std::char_traits<char>, std::allocator<char>>::pointer")
@FFIGen
@CXXHead(
        system = "string"
)
public interface PointerCharStdCharTraitsCharStdAllocatorChar extends CXXPointer {
    @FFIExpr("{0}")
    PointerStdAllocatorChar get();

    static PointerCharStdCharTraitsCharStdAllocatorChar cast(final long __foreign_address) {
        try {
            Class<PointerCharStdCharTraitsCharStdAllocatorChar> clz = (Class<PointerCharStdCharTraitsCharStdAllocatorChar>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(PointerCharStdCharTraitsCharStdAllocatorChar.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static PointerCharStdCharTraitsCharStdAllocatorChar cast(final FFIPointer __foreign_pointer) {
        return PointerCharStdCharTraitsCharStdAllocatorChar.cast(__foreign_pointer.getAddress());
    }
}
