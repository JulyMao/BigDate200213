package com.me;

/**
 * @author maow
 * @create 2020-04-29 19:09
 */

/**
 * 清洗谷粒影音的原始数据
 * 清洗规则
 * H-ucblRMjuY	nevrknw	735	People & Blogs	22	299935	2.67	640	213	9jEZyxFs1C0	PBvopqeKYlM	jiExVINzBzk	2d5fjIDafH4	-_gfF-hlGAA	LOdSctUe6Gg	xC8kNCiFcOc	uAaQdthRYEw
 *  1. 将数据长度小于9的清洗掉
 *  2. 将数据中的视频类别中间的空格去掉   People & Blogs
 *  3. 将数据中的关联视频id通过&符号拼接
 */

public class ETLUtil {

    public static String filterString(String str){
        StringBuffer sb = new StringBuffer();
        String[] split = str.split("\t");
        if (split.length < 9){
            return  null;
        }
        if (split.length == 9) {
            split[3] = split[3].replace(" & ","&");
            for (int i = 0 ; i < split.length; i++) {
                if (i < 8) {
                    sb.append(split[i] + "\t");
                }else{
                    sb.append(split[i]);
                }
            }
        }else if(split.length > 9){
            split[3] = split[3].replace(" & ","&");
            for (int i = 0; i < split.length; i++) {
                if (i <= 8){
                    sb.append(split[i]+"\t");
                }else if (i < split.length - 1){
                    sb.append(split[i] + "&");
                }else{
                    sb.append(split[i]);
                }
            }
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        String s = filterString("H-ucblRMjuY\tnevrknw\t735\tPeople & Blogs\t22\t299935\t2.67\t640\t213\t9jEZyxFs1C0\tPBvopqeKYlM\tjiExVINzBzk\t2d5fjIDafH4\t-_gfF-hlGAA\tLOdSctUe6Gg\txC8kNCiFcOc\tuAaQdthRYEw");
        System.out.println(s);

    }
}
