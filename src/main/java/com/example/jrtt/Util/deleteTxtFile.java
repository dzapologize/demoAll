package com.example.jrtt.Util;

import java.io.File;

public class deleteTxtFile {

    //删除指定文件路径的文件File f为路径，例如：File file=new File("D:\\txt\\");
    public static void delete(File f)
    {
        //数组指向文件夹中的文件和文件夹
        File[] fi=f.listFiles();
        //遍历文件和文件夹
        for(File file:fi)
        {
            //如果是文件夹，递归查找
            if(file.isDirectory())
                delete(file);
            else if(file.isFile())
            {
                //是文件的话，把文件名放到一个字符串中
                String filename=file.getName();
                //如果是“class”后缀文件，返回一个boolean型的值
                if(filename.endsWith("txt"))
                {
                    System.out.println("成功删除：："+file.getName());
                    file.delete();
                }
            }
        }
    }
}
