package com.codeh.tree;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className BinaryTreeDemo
 * @date 2021/8/3 17:35
 * @description 二叉树的遍历：前序，中序，后序
 */
public class BinaryTreeDemo {
    public static void main(String[] args) {

        // 先创建一颗二叉树
        BinaryTree binaryTree = new BinaryTree();

        // 创建所需要的节点
        HeroNode root = new HeroNode(1, "宋江");
        HeroNode node2 = new HeroNode(2, "吴用");
        HeroNode node3 = new HeroNode(3, "卢俊义");
        HeroNode node4 = new HeroNode(4, "林冲");
        HeroNode node5 = new HeroNode(5, "关羽");

        // 手动创建二叉树
        root.setLeft(node2);
        root.setRight(node3);
        node3.setRight(node4);
        node3.setLeft(node5);

        binaryTree.setRoot(root);

        System.out.println("前序遍历"); // 1, 2, 3, 5, 4
        binaryTree.preOrder();

        System.out.println("中序遍历"); // 2, 1, 5, 3, 4
        binaryTree.infixOrder();

        System.out.println("后序遍历"); // 2, 5, 4, 3, 1
        binaryTree.postOrder();

    }
}

class BinaryTree {
    // 头节点
    private HeroNode root;

    public void setRoot(HeroNode root) {
        this.root = root;
    }

    // 前序遍历
    public void preOrder() {
        if (this.root != null) {
            this.root.preOrder();
        } else {
            System.out.println("二叉树为空,无法遍历");
        }
    }

    // 中序遍历
    public void infixOrder() {
        if (this.root != null) {
            this.root.infixOrder();
        } else {
            System.out.println("二叉树为空,无法遍历");
        }
    }

    // 后序遍历
    public void postOrder() {
        if (this.root != null) {
            this.root.postOrder();
        } else {
            System.out.println("二叉树为空,无法遍历");
        }
    }
}

class HeroNode {
    private int no;
    private String name;
    private HeroNode left;
    private HeroNode right;

    public HeroNode(int no, String name) {
        this.no = no;
        this.name = name;
    }

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HeroNode getLeft() {
        return left;
    }

    public void setLeft(HeroNode left) {
        this.left = left;
    }

    public HeroNode getRight() {
        return right;
    }

    public void setRight(HeroNode right) {
        this.right = right;
    }

    @Override
    public String toString() {
        return "HeroNode{" +
                "no=" + no +
                ", name='" + name + '\'' +
                '}';
    }

    // 前序遍历的方法
    public void preOrder() {
        System.out.println(this); // 输出父节点

        // 递归向左子树前序遍历
        if (this.left != null) {
            this.left.preOrder();
        }

        // 递归向右子树前序遍历
        if (this.right != null) {
            this.right.preOrder();
        }
    }

    // 中序遍历
    public void infixOrder() {
        // 递归向左子树中序遍历
        if (this.left != null) {
            this.left.infixOrder();
        }

        // 输出父节点
        System.out.println(this);

        // 递归向右子树中序遍历
        if (this.right != null) {
            this.right.infixOrder();
        }
    }

    // 后续遍历
    public void postOrder() {
        // 递归向左子树后序遍历
        if (this.left != null) {
            this.left.postOrder();
        }

        // 递归向右子树后序遍历
        if (this.right != null) {
            this.right.postOrder();
        }

        System.out.println(this);
    }
}
