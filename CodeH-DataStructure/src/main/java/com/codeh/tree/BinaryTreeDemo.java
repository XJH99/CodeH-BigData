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

//        System.out.println("前序遍历"); // 1, 2, 3, 5, 4
//        binaryTree.preOrder();
//
//        System.out.println("中序遍历"); // 2, 1, 5, 3, 4
//        binaryTree.infixOrder();
//
//        System.out.println("后序遍历"); // 2, 5, 4, 3, 1
//        binaryTree.postOrder();

        System.out.println("前序遍历方式~~");

        HeroNode resNode = binaryTree.preOrderSearch(5);
        if (resNode != null) {
            System.out.printf("找到了，信息为 no=%d name=%s", resNode.getNo(), resNode.getName());
        } else {
            System.out.printf("没有找到 no = %d的英雄", 5);
        }

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

    /**
     * 前序遍历查找
     *
     * @param no 查找编号
     * @return
     */
    public HeroNode preOrderSearch(int no) {
        if (this.root != null) {
            return root.preOrderSearch(no);
        } else {
            return null;
        }
    }

    /**
     * 中序遍历查找
     *
     * @param no 查找编号
     * @return
     */
    public HeroNode infixOrderSearch(int no) {
        if (this.root != null) {
            return root.infixOrderSearch(no);
        } else {
            return null;
        }
    }

    /**
     * 后序遍历查找
     *
     * @param no 查找编号
     * @return
     */
    public HeroNode postOrderSearch(int no) {
        if (this.root != null) {
            return root.postOrderSearch(no);
        } else {
            return null;
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

    /**
     * 前序遍历查找
     *
     * @param no 查找的编号
     * @return 有就返回Node, 没有就返回null
     */
    public HeroNode preOrderSearch(int no) {
        System.out.println("进入前序遍历");

        // 当前节点是不是
        if (this.no == no) {
            return this;
        }

        // 判断当前节点的左子节点是否为空，如果不为空，则递归前序查找
        // 如果左递归前序查找，找到节点，则返回
        HeroNode resNode = null;
        if (this.left != null) {
            resNode = this.left.preOrderSearch(no);
        }

        if (resNode != null) { // 说明左子树已查到
            return resNode;
        }

        // 左节点没有查到，查右节点；右节点不为空，继续向右递归前序查询
        if (this.right != null) {
            resNode = this.right.preOrderSearch(no);
        }

        return resNode;
    }

    /**
     * 中序遍历查找
     *
     * @param no 查找的编号
     * @return 有就返回Node, 没有就返回null
     */
    public HeroNode infixOrderSearch(int no) {
        // 判断当前节点的左子节点是否为空，如果不为空，则递归前序查找
        // 如果左递归中序查找，找到节点，则返回
        HeroNode resNode = null;
        if (this.left != null) {
            resNode = this.left.infixOrderSearch(no);
        }

        if (resNode != null) { // 说明左子树已查到
            return resNode;
        }
        System.out.println("进入中序遍历");

        // 当前节点是不是
        if (this.no == no) {
            return this;
        }

        // 左节点没有查到，查右节点；右节点不为空，继续向右递归中序查询
        if (this.right != null) {
            resNode = this.right.infixOrderSearch(no);
        }

        return resNode;
    }

    /**
     * 后序遍历查找
     *
     * @param no 查找的编号
     * @return 有就返回Node, 没有就返回null
     */
    public HeroNode postOrderSearch(int no) {
        // 判断当前节点的左子节点是否为空，如果不为空，则递归前序查找
        // 如果左递归中序查找，找到节点，则返回
        HeroNode resNode = null;
        if (this.left != null) {
            resNode = this.left.postOrderSearch(no);
        }

        if (resNode != null) { // 说明左子树已查到
            return resNode;
        }

        // 左节点没有查到，查右节点；右节点不为空，继续向右递归中序查询
        if (this.right != null) {
            resNode = this.right.postOrderSearch(no);
        }

        if (resNode != null) {
            return resNode;
        }

        System.out.println("进入后序遍历");

        // 当前节点是不是
        if (this.no == no) {
            return this;
        }

        return null;
    }

}
