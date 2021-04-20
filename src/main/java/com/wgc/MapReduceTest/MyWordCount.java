package com.wgc.MapReduceTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author wanggc
 * @date 2019/09/11 星期三 22:10
 */
public class MyWordCount {

    /**
     * 编写 Map 程序
     * Hadoop MapReduce 框架已经在类 Mapper 中实现了 Map 任务的基本功能。
     * 为了实现 Map 任务，开发者只需要继承类 Mapper，并实现该类的 Map 函数。
     */
    public static class MyMapper extends Mapper<Object, Text,Text, IntWritable> {
        //将需要输出的两个变量 one 和 label 进行初始化
        //变量 one 的初始值直接设置为 1，表示某个单词在文本中出现过
        private final static IntWritable one = new IntWritable(1);
        /*
        * Text:此类使用标准UTF8编码存储文本。 它提供了在字节级序列化，反序列化和比较文本的方法。
        * 长度类型为整数，并使用零压缩格式进行序列化。
        * 此外，它提供了字符串遍历的方法，而无需将字节数组转换为字符串。
        * 还包括用于序列化/解析字符串，编码/解码字符串，检查字节数组是否包含有效UTF8代码，
        * 计算编码字符串长度的实用程序。
        * */
        private Text word = new Text();

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            /*
            * StringTokenizer类：根据自定义字符为分界符进行拆分，并将结果进行封装提供对应方法进行遍历取值，
            *  StringTokenizer 方法不区分标识符、数和带引号的字符串，它们也不识别并跳过注释；
            * 该方法用途类似于split方法，只是对结果进行了封装；
            * */
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(value, one);
            }
        }
    }

    /* Map端 map任务输出结果：
     * file001/Map1:<"Hello",1> ,<"world",1>,<"Connected",1>,<"world",1>
     * file002/Map2:<"One",1>,<"world",1>,<"One",1>,<"dream",1>
     * file003/Map3:<"Hello",1>,<"Hadoop",1>,<"Hello",1>,<"Map",1>,<"Hello", 1> ,<"Reduce",1>
    * */

    /*
    在执行完 Map 函数之后，会进入 Shuffle 阶段，在这个阶段中，
    MapReduce 框架会自动将 Map 阶段的输出结果进行排序和分区，然后再分发给相应的 Reduce 任务去处理。
    * Map 端 Shuffle 阶段输出结果：
    * file001/Map1：<"Connected",1> ，<"Hello", 1>，<"world",<1,1>>
    * file002/Map2：<"dream",1>，<"One", <1, 1>>，<"world", 1>
    * file003/Map3：<"Map", 1>，<"Hadoop",1>，<"Hello",<1,1,1>>，<"Reduce", 1>
    * */

    /**
     * 编写 Reduce 程序
     * 编写 MapReduce 程序的第二个任务就是编写 Reduce 程序。
     * 在单词计数任务中，Reduce 需要完成的任务就是把输入结果中的数字序列进行求和，从而得到每个单词的出现次数
     */
    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            //设置 sum 参数用来记录每个单词的出现次数
            int sum = 0;
            //遍历 value 列表，并对其中的数字进行累加，最终就可以得到每个单词总的出现次数
            for (IntWritable val:values){
                sum += val.get();
            }
            result.set(sum);
            //在输出的时候，仍然使用 context 类型的变量存储信息。当 Reduce 阶段结束时，就可以得到最终需要的结果
            context.write(key, result);
        }
    }
/*
* Reduce端Shuffle阶段输出结果
* <"Connected",1>
< "dream",1>
<"Hadoop",1>
<"Hello",<1,1,1,1>>
<"Map",1>
<"One",<1,1>>
<"world", <1,1,1>>
<"Reduce", 1>
* */

/*
* Reduce 任务输出结果
*   <"dream", 1>
 	<"Hadoop", 1>
 	<"Hello", 4>
 	<"Map", 1>
 	<"One", 2>
 	<"world", 3>
 	<"Reduce", 1>
* */


}
