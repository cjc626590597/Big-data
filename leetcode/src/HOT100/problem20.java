package HOT100;

import java.util.Stack;


class Solution20 {
    public boolean isValid(String s) {
        char[] chars = s.toCharArray();
        Stack stack = new Stack();
        for (char c:chars) {
            if (c == '{' || c == '(' || c == '[' ){
                stack.push(c);
            }else {
                if (stack.empty()){
                    return false;
                }
                if (c == '}'){
                    if ((char)stack.peek()=='{'){
                        stack.pop();
                    }else {
                        return false;
                    }
                }else if(c == ')'){
                    if ((char)stack.peek()=='('){
                        stack.pop();
                    }else {
                        return false;
                    }
                }else if(c == ']'){
                    if ((char)stack.peek()=='['){
                        stack.pop();
                    }else {
                        return false;
                    }
                }
            }
        }
        if (stack.empty()){
            return true;
        }
        return false;
    }
}

public class problem20{
    public static void main(String[] args) {
        String s = "()[]{}";
        System.out.println(new Solution20().isValid(s));

    }
}