import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;

@SpringBootApplication
public class DemoApplication {
//
//
//
//    public static void main(String[] args) throws IOException {
//        SpringApplication.run(DemoApplication.class, args);
//
//        long maxMemory=Runtime.getRuntime().maxMemory();
//        long totalMemory=Runtime.getRuntime().totalMemory();
//        System.out.println("MAX_MEMORY="+maxMemory+"(字节)"+(maxMemory/(double)1024/1024)+"MB");
//        System.out.println("TOTAL_MEMORY="+totalMemory+"(字节)"+(totalMemory/(double)1024/1024)+"MB");
//        System.out.println("************hello GC");
////        testSpark testSpark = new testSpark();
////        testSpark.sparkQuery();
////        //计时
////        StopWatch stopWatch = new StopWatch();
////        stopWatch.start();
//////        queryHbase queryHbase = new queryHbase();
//////        List<String> allRowkey = queryHbase.getAllRowkey();
//////        Map<String, JSONObject> allJson = queryHbase.getAllJson(allRowkey);
//////        System.out.println(allJson.size());
////        queryHbaseMySQL rowkeyList = new queryHbaseMySQL();
////        System.out.println(rowkeyList);
////        stopWatch.stop();
//
////        System.out.println("统计耗时："+(stopWatch.getTotalTimeMillis())/1000+"秒");
////        queryHbase queryHbase = new queryHbase();
// //       List<String> allRowkey = queryHbase.getAllRowkey();
// //       Map<String, JSONObject> allJson = queryHbase.getAllJson(allRowkey);
////        otherRowkey otherRowkey = new otherRowkey();
////        List<String> strings = otherRowkey.RowkeyListOther();
////        System.out.println("23日的RowkeyListOther的长度为： "+ strings.size());
//    }
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            System.out.println("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

        };
    }

}
