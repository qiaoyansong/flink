package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ：Qiao Yansong
 * @date ：Created in 2023/12/31 10:38 上午
 * description：
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据(相对路径会报错)
        String inputPath = "/Users/didi/Documents/ideaWorkPlace/test_workplace/flink/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计 注意flatmap不能使用lambda 否则执行会报错
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                // 按照第一个位置的word分组
                .groupBy(0)
                // 按照第二个位置上的数据求和
                .sum(1);
        resultSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                out.collect(new Tuple2<>(str, 1));
            }
        }
    }
}
