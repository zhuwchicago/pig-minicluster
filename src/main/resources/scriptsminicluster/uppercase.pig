A = load '/tmp/lowercase.data';
B = foreach A generate flatten(TOKENIZE((chararray)$0)) as word;
C = foreach B generate FLATTEN(org.apache.pig.extension.pigudf.ToUpperCase(*));
dump C;