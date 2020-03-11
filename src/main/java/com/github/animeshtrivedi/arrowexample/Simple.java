package com.github.animeshtrivedi.arrowexample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.IntWriter;



// Sample : https://github.com/dremio/dremio-oss/blob/04e0387d474f1408731da0029aef7ecfad5e4d08/sabot/kernel/src/test/java/com/dremio/exec/vector/complex/writer/TestRepeated.java
// Sample : https://github.com/dremio/dremio-oss/blob/04e0387d474f1408731da0029aef7ecfad5e4d08/sabot/kernel/src/test/java/com/dremio/exec/vector/complex/writer/TestPromotableWriter.java
// Sample : http://mail-archives.eu.apache.org/mod_mbox/arrow-commits/201906.mbox/%3C156023133603.5352.16667062394388421918@gitbox.apache.org%3E
public class Simple {
    public static void main(String[] args) throws JsonProcessingException {
//        List<Field> children
//        Field structField = new Field("a1", new FieldType(true, new ArrowType.Map(true), null),Lists.newArrayList(new Field("a3", new ArrowType.Struct.INSTANCE)));
//        Field structField2 = new Field("col", new FieldType(true, new ArrowType.Struct(), null),
//                Lists.newArrayList(Field.nullable("a3", new ArrowType.Bool())));

//        ImmutableList.Builder<Field> builder = ImmutableList.builder();
//        builder.add(structField);
//
//
//        Schema schema = new Schema(builder.build(), null);
//
//        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));
//
//        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
//
//        FieldVector vector = root.getVector("col");
//        MapVector v = (MapVector) vector;



//        Test
        BufferAllocator allocator;
        allocator = new RootAllocator(Long.MAX_VALUE);

        NonNullableStructVector structVector = new NonNullableStructVector("", allocator, null);
        ComplexWriterImpl writer = new ComplexWriterImpl("col", structVector);
        StructWriter struct = writer.rootAsStruct();

        {
            struct.start();

            final ListWriter list = struct.list("a");
            list.startList();

            ListWriter innerList = list.list();
            IntWriter innerInt = innerList.integer();

            innerList.startList();
            innerInt.writeInt(1);
            innerInt.writeInt(2);
            innerInt.writeInt(3);
            innerList.endList();

            innerList.startList();
            innerInt.writeInt(4);
            innerInt.writeInt(5);
            innerList.endList();

            list.endList();

            struct.integer("nums").writeInt(14);

            StructWriter repeatedMap = struct.list("b").struct();
            repeatedMap.start();
            repeatedMap.integer("c").writeInt(1);
            repeatedMap.end();

            repeatedMap.start();
            repeatedMap.integer("c").writeInt(2);
            repeatedMap.bigInt("x").writeBigInt(15);
            repeatedMap.end();

            struct.end();
        }

        writer.setPosition(1);
        {
            struct.start();

            ListWriter list = struct.list("a");
            list.startList();

            ListWriter innerList = list.list();
            IntWriter innerInt = innerList.integer();

            innerList.startList();
            innerInt.writeInt(-1);
            innerInt.writeInt(-2);
            innerInt.writeInt(-3);
            innerList.endList();

            innerList.startList();
            innerInt.writeInt(-4);
            innerInt.writeInt(-5);
            innerList.endList();

            list.endList();

            struct.integer("nums").writeInt(-28);

            struct.list("b").startList();
            StructWriter repeatedMap = struct.list("b").struct();
            repeatedMap.start();
            repeatedMap.integer("c").writeInt(-1);
            repeatedMap.end();

            repeatedMap.start();
            repeatedMap.integer("c").writeInt(-2);
            repeatedMap.bigInt("x").writeBigInt(-30);
            repeatedMap.end();
            struct.list("b").endList();

            struct.end();
        }
        writer.setValueCount(2);

        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        System.out.println("Map of Object[0]: " + ow.writeValueAsString(structVector.getObject(0)));
        System.out.println("Map of Object[1]: " + ow.writeValueAsString(structVector.getObject(1)));


    }


}

//    Field(Class<?> declaringClass,
//          String name,
//          Class<?> type,
//          int modifiers,
//          int slot,
//          String signature,
//          byte[] annotations)
//    {