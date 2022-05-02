import java.io.*;
public class Main {
    /** Print the sum of two integers read from the keyboard */
    public static void main(String[] args) {
        System.out.println("Enter a number: ");
        String s="7";
        int a,b;

        try{
            a=Integer.parseInt(s);
            System.out.println(a);

        }
        catch(Exception e){
            System.out.println("NumberFormatException");
            a=0;

        }
        System.out.println("Enter another number: ");
        String S1="5";
        try{
            b= Integer.parseInt(S1);
            System.out.println(b);
        }
        catch(Exception e2){
            System.out.println( "NumberFormatException");
            b=0;

        }
        System.out.println("Product: " + a*b);
    }
}
public class Main {
    public static void main(String[] args) {
        try {
            int[] myNumbers = {1, 2, 3};
            System.out.println(myNumbers[10]);
        } catch (Exception e) {
            System.out.println("Something went wrong.");
        } finally {
            System.out.println("The 'try catch' is finished.");
        }
    }
}
