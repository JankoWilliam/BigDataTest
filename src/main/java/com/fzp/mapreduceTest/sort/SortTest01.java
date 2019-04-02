package com.fzp.mapreduceTest.sort;

public class SortTest01 {

    public static void main(String[] args) {
        int [] nums = {1,3,5,12,20,44,56,98};
        System.out.println(binarySearch(nums,21));
    }


    public static int binarySearch(int srcArray [] ,int des){
        int low=0;
        int height=srcArray.length-1;
        while(low<=height){
            int middle=(low+height)/2;
            if(des==srcArray[middle]){
                return middle;
            }else if(des<srcArray[middle]){
                height=middle-1;
            }else{
                low=middle+1;
            }
        }

        return -1*low;
    }
    private static int[] insertElement(int original[],
                                       int element, int index) {
        int length = original.length;
        int destination[] = new int[length + 1];
        System.arraycopy(original, 0, destination, 0, index);
        destination[index] = element;
        System.arraycopy(original, index, destination, index
                + 1, length - index);
        return destination;
    }
}
