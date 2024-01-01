package wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
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

        // 路径必须是绝对路径 or 项目根路径开始的相对路径
        String inputPath = "src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        FlatMapOperator<String, Tuple2<String, Long>> line2WordCountTupleOp = inputDataSet.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 按空格分词
            String[] words = line.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                out.collect(Tuple2.of(str, 1L));
            }
        })
                // 如果flatmap使用lambda表达式 必须显式只订购返回值类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 按照第一个位置的word分组
        UnsortedGrouping<Tuple2<String, Long>> word2CountMap = line2WordCountTupleOp.groupBy(0);

        // 按照第二个位置上的数据求和
        AggregateOperator<Tuple2<String, Long>> resultSet = word2CountMap.sum(1);
        resultSet.print();
    }

}
