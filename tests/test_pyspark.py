import os
import duckdb
import ibis
from ibis_substrait.compiler.core import SubstraitCompiler
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sparktrait.substrait.visit import RelVisitor, SubstraitPlanEditor, visit_and_update
from substrait.gen.proto.plan_pb2 import Plan as SubstraitPlan
from substrait.gen.proto.algebra_pb2 import AggregateRel, CrossRel, FetchRel, FilterRel, HashJoinRel, JoinRel, MergeJoinRel, ProjectRel, ReadRel, SetRel, SortRel


def test_basic(tmp_path):
    data = [
        pa.array([1, 2, 3]),
        pa.array(["a", "b", "c"])
    ]
    
    more_data = [
        pa.array([4, 5, 6]),
        pa.array(["d", "e", "f"])
    ]
    names = ["c1", "c2"]
    table_1 = pa.Table.from_arrays(data, names=names)
    table_2 = pa.Table.from_arrays(more_data, names=names)
    file_path_1 = tmp_path / "sample1.parquet"
    file_path_2 = tmp_path / "sample2.parquet"
    pq.write_table(table_1, file_path_1)
    pq.write_table(table_2, file_path_2)
    tb1 = pq.read_table(file_path_1)
    tb2 = pq.read_table(file_path_2)
    assert tb1.equals(table_1)
    assert tb2.equals(table_2)
    
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(str(file_path_1), str(file_path_2))
    pdf = df.toPandas()
    merged_df = pd.concat([tb1.to_pandas(), tb2.to_pandas()], ignore_index=True)
    assert pdf.equals(merged_df)


def test_spark_substrait_basic(tmp_path):
    data = [
        pa.array([1, 2, 3]),
        pa.array(["a", "b", "c"])
    ]
    
    more_data = [
        pa.array([4, 5, 6]),
        pa.array(["d", "e", "f"])
    ]
    names = ["c1", "c2"]
    table_1 = pa.Table.from_arrays(data, names=names)
    table_2 = pa.Table.from_arrays(more_data, names=names)
    file_path_1 = tmp_path / "sample1.parquet"
    file_path_2 = tmp_path / "sample2.parquet"
    pq.write_table(table_1, file_path_1)
    pq.write_table(table_2, file_path_2)
    tb1 = pq.read_table(file_path_1)
    tb2 = pq.read_table(file_path_2)
    assert tb1.equals(table_1)
    assert tb2.equals(table_2)
    
    merged_df = pd.concat([tb1.to_pandas(), tb2.to_pandas()], ignore_index=True)
    select_df = merged_df["c1"]
    
    tb = ibis.table([("c1", "int64"), ("c2", "varchar")], "sample",)
    query = tb.select(["c1"])
    compiler = SubstraitCompiler()
    plan = compiler.compile(query).SerializeToString()
    
    substrait_plan = SubstraitPlan()
    substrait_plan.ParseFromString(plan)
    

    class RelUpdateVisitor(RelVisitor):
        
        def __init__(self, files, formats):
            self._files = files
            self._formats = formats
        
        def visit_aggregate(self, rel: AggregateRel):
            pass
        
        def visit_cross(self, rel: CrossRel):
            pass
        
        def visit_fetch(self, rel: FetchRel):
            pass
        
        def visit_filter(self, rel: FilterRel):
            pass
        
        def visit_join(self, rel: JoinRel):
            pass
        
        def visit_hashjoin(self, rel: HashJoinRel):
            pass
        
        def visit_merge(self, rel: MergeJoinRel):
            pass
        
        def visit_project(self, rel: ProjectRel):
            pass
        
        def visit_read(self, read_rel: ReadRel):
            local_files = read_rel.LocalFiles()
            for file, file_format in zip(self._files, self._formats):
                file_or_files = local_files.FileOrFiles()
                file_or_files.uri_file = file
                if file_format == "orc":
                    orc = ReadRel.LocalFiles.FileOrFiles.OrcReadOptions()
                    file_or_files.orc.CopyFrom(orc)
                elif file_format == "dwrf":
                    dwrf = ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions()
                    file_or_files.dwrf.CopyFrom(dwrf)
                elif file_format == "parquet":
                    parquet = ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions()
                    file_or_files.parquet.CopyFrom(parquet)
                elif file_format == "arrow":
                    arrow = ReadRel.LocalFiles.FileOrFiles.ArrowReadOptions()
                    file_or_files.arrow.CopyFrom(arrow)
                else:
                    raise ValueError(f"Unsupported file format {file_format}")
                local_files.items.append(file_or_files)
            read_rel.local_files.CopyFrom(local_files)
        
        def visit_set(self, rel: SetRel):
            pass
        
        def visit_sort(self, rel: SortRel):
            pass

    # we would assume a scenario where Spark Dataframe is created from a file on disk
    # to simulate that, we would replace the named_table with local_files in the plan generated
    # via Ibis-Substrait.
    rel_visitor = RelUpdateVisitor(files=[str(file_path_1), str(file_path_2)], formats=["parquet", "parquet"])
    editor = SubstraitPlanEditor(substrait_plan.SerializeToString())
    visit_and_update(editor.rel, rel_visitor)
    
    # create Spark dataframe via Substrait
    
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    
    class TestSubstraitToSpark(RelVisitor):
        
        def __init__(self, spark_session):
            self._spark = spark_session
            self._df = None
            self._base_schema = None
            
        @property
        def dataframe(self):
            return self._df
        
        def visit_aggregate(self, rel: AggregateRel):
            pass
        
        def visit_cross(self, rel: CrossRel):
            pass
        
        def visit_fetch(self, rel: FetchRel):
            pass
        
        def visit_filter(self, rel: FilterRel):
            pass
        
        def visit_join(self, rel: JoinRel):
            pass
        
        def visit_hashjoin(self, rel: HashJoinRel):
            pass
        
        def visit_merge(self, rel: MergeJoinRel):
            pass
        
        def visit_project(self, project_rel: ProjectRel):
            from substrait.gen.proto.algebra_pb2 import Expression
            if project_rel.expressions:
                expressions = project_rel.expressions
                for expr in expressions:
                    if expr.HasField("selection"): # FieldReference
                        selection = expr.selection
                        # case: direct_reference
                        if selection.HasField("direct_reference"):
                            direct_reference = selection.direct_reference
                            ## oneof reference_type {
                            ##       MapKey map_key = 1;
                            ##       StructField struct_field = 2;
                            ##       ListElement list_element = 3;
                            ## }
                            if direct_reference.HasField("map_key"):
                                raise NotImplemented("MapKey not implemented in Project expression")
                            elif direct_reference.HasField("list_element"):
                                raise NotImplemented("MapKey not implemented in Project expression")
                            if direct_reference.HasField("struct_field"):
                                struct_field = direct_reference.struct_field
                                selection_fields = []
                                if self._base_schema:
                                    col_names = self._base_schema.names
                                    selection_fields.append(col_names[struct_field.field])
                                self._df = self._df.select(selection_fields)
                        # case: masked_reference
                        elif selection.HasField("masked_reference"):
                            return NotImplemented(f"MaskedExpression not supported in ProjectRel")
                        else:
                            return ValueError("Unsupported FieldReference type")
        
        def visit_read(self, read_rel: ReadRel):
            self._base_schema = read_rel.base_schema
            df = None
            local_files = read_rel.local_files
            for _, file in enumerate(local_files.items):
                file_path =  file.uri_file
                if file.HasField("parquet"):
                    if df is None:
                        df = spark.read.parquet(file_path)
                    else:
                        df = df.union(spark.read.parquet(file_path))
                elif file.HasField("orc"):
                    raise NotImplemented("Reading Arrow files not yet supported")
                elif file.HasField("dwrf"):
                    raise NotImplemented("Reading Arrow files not yet supported")
                elif file.HasField("arrow"):
                    raise NotImplemented("Reading Arrow files not yet supported")
            self._df = df
        
        def visit_set(self, rel: SetRel):
            pass
        
        def visit_sort(self, rel: SortRel):
            pass
        
    new_editor = SubstraitPlanEditor(editor.plan.SerializeToString())
    spark_visitor = TestSubstraitToSpark(spark_session=spark)
    visit_and_update(new_editor.rel, spark_visitor)
    df = spark_visitor.dataframe
    out_pdf = df.toPandas()
    out_pdf = out_pdf["c1"] # make sure we get a series since select_df is a series
    assert out_pdf.equals(select_df)
