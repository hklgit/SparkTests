package others

import org.apache.spark.sql.SQLContext

/**
  * Created by Kerven-HAN on 2019/9/2 14:16.
  * Talk is cheap , show me the code 
  */
object JsonParseArray {




  def main(args: Array[String]): Unit = {


    for(StructField sf:df.schema().fields()){
      this.li.add(sf.name());
      String sname=StringUtils.join(li.toArray(),"_");
      if(sf.dataType().typeName()=="array"||"array".equals(sf.dataType().typeName())){

        ArrayType at=(ArrayType)sf.dataType();
        df=df.withColumn(sname, functions.explode(functions.when(df.col(sname).isNull(), functions.array(functions.lit(null).cast(at.elementType())))
          .when(functions.size(df.col(sname)).equalTo(0), functions.array(functions.lit(null).cast(at.elementType())))
          .otherwise(df.col(sname))));
        df=array_loop((ArrayType)sf.dataType(),df);
      }else if(sf.dataType().typeName()=="struct"||"struct".equals(sf.dataType().typeName())){
        df=struct_loop((StructType)sf.dataType(),df);
        df=df.drop(sname);
      }
      cols.add(StringUtils.join(li.toArray(),"."));
      this.li.remove(sf.name());
    }


    public DataFrame array_loop(ArrayType arr,DataFrame df){
      if(arr.elementType().typeName()=="struct"||"struct".equals(arr.elementType().typeName())){
        df=struct_loop((StructType)arr.elementType(),df);
      }
      return df;
    }

    public DataFrame struct_loop(StructType st,DataFrame df){
      for(StructField sf:st.fields()){
        this.li.add(sf.name());
        String sname=StringUtils.join(li.toArray(),"_");
        String sname1=StringUtils.join(li.toArray(),".");
        if(sf.dataType().typeName()=="array"||"array".equals(sf.dataType().typeName())){

          ArrayType at=(ArrayType)sf.dataType();
          df=df.withColumn(sname, functions.explode(functions.when(df.col(sname).isNull(), functions.array(functions.lit(null).cast(at.elementType())))
            .when(functions.size(df.col(sname)).equalTo(0), functions.array(functions.lit(null).cast(at.elementType())))
            .otherwise(df.col(sname))));
          df=array_loop((ArrayType)sf.dataType(),df);
        }else if(sf.dataType().typeName()=="struct"||"struct".equals(sf.dataType().typeName())){
          df=struct_loop((StructType)sf.dataType(),df);
        }
        df=df.selectExpr(sname1+" as "+sname,"*");
        cols.add(StringUtils.join(li.toArray(),"."));
        this.li.remove(sf.name());
      }
      return df;
    }


  }

}
