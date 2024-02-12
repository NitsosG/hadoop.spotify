package aueb.fnitsos;

public class TestMain {
    public static void main(String[] args){
        String test = "SINVERG?ENZA - con Angela Torres|||0.729";
        String[] parts = test.split("|||");
        System.out.print(parts[1]);
    }
}
