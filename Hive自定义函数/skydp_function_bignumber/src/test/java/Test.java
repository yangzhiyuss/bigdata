import java.util.Scanner;

public class Test {
    public static ListNode reverse(ListNode head) {
        ListNode currentNode = head.next;
        head.next = null;

        while (currentNode != null) {
            ListNode tmpNode = currentNode;
            currentNode = currentNode.next;
            tmpNode.next = head.next;
            head.next = tmpNode;
        }

        return head;
    }

}

class ListNode {
    public int data;
    ListNode next;
}

