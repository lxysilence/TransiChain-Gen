package TripChain.generator;

import java.io.*;
import java.util.*;
import java.util.Deque;
import java.util.IdentityHashMap;

import static TripChain.generator.laplaceNoise.generateLaplaceRandom;

public class Code02_TrieTree_RCDP {

    public Node root;
    private int originalSize; // 新增字段，记录原始数据总数

    //构造函数，初始化根节点
    public Code02_TrieTree_RCDP() {
        this.root = new Node();
        this.originalSize = 0;
    }

    // 定义内部类，表示树的节点
    public static class Node {
        public int pass; // 经过该节点的轨迹数量
        public HashMap<String, Node> nexts; // 子节点映射

        public Node() {
            this.pass = 0;
            this.nexts = new HashMap<>();
        }

        // 深拷贝构造函数
        public Node(Node other) {
            this.pass = other.pass;
            this.nexts = new HashMap<>();
            for (Map.Entry<String, Node> entry : other.nexts.entrySet()) {
                this.nexts.put(entry.getKey(), new Node(entry.getValue()));
            }
        }
    }

    // 插入轨迹数据
    public void insert(String word) {
        if (word == null || word.isEmpty()) return;

        String[] parts = word.split(",");
        Node node = root;
        node.pass++; // 根节点pass+1
        originalSize++; // 总轨迹数+1

        for (String part : parts) {
            if (!node.nexts.containsKey(part)) {
                node.nexts.put(part, new Node());
            }
            node = node.nexts.get(part);
            node.pass++; // 路径节点pass+1
        }

        // 注意：此处不再设置end值
    }

    //序列总数
    public int size() {
        return originalSize;
    }

    // 深拷贝整棵树
    public Code02_TrieTree_RCDP copyTree() {
        Code02_TrieTree_RCDP copy = new Code02_TrieTree_RCDP();
        copy.root = new Node(this.root);
        copy.originalSize = this.originalSize;
        return copy;
    }


    //指数分配噪声
//    // 常量：最大深度（根据轨迹数据，最大长度为4，所以最大深度为4）
//    private static final int MAX_DEPTH = 4;
//    // 常量：调和级数总权重（深度0~4: 1 + 1/2 + 1/3 + 1/4 + 1/5）
//    private static final double TOTAL_WEIGHT = 1 + 1.0/2 + 1.0/3 + 1.0/4 + 1.0/5; // ≈2.283
//
//    // 公有方法：启动添加噪声过程
//    public void addNoise(double epsilon) {
//        traverseAndAddNoise(root, 0, epsilon);
//    }
//
//    // 递归添加噪声（修改后）
//    private void traverseAndAddNoise(Node node, int level, double epsilon) {
//        if (node == null) return;
//
//        // 1. 计算当前层级的隐私预算
//        double weight = 1.0 / (level + 1); // 权重与深度成反比
//        double epsilon_l = epsilon * weight / TOTAL_WEIGHT; // 按权重分配预算
//
//        // 2. 计算Laplace噪声（灵敏度s=1）
//        double lambda = 1.0 / epsilon_l;
//        double noise = Math.round(generateLaplaceRandom(lambda));
//
//        // 3. 添加噪声并处理负值
//        node.pass += noise;
//        if (node.pass < 0) node.pass = 0;
//
//        // 4. 递归处理子节点
//        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
//            traverseAndAddNoise(entry.getValue(), level + 1, epsilon);
//        }
//    }


    //比例分配
    // 常量：最大深度（根据轨迹数据，最大长度为4，所以最大深度为4）
    private static final int MAX_DEPTH = 4;

    // 隐私预算分配比例（深度0~4）
    private static final double[] EPSILON_RATIOS = {0.25, 0.25, 0.25, 0.15, 0.1};

    // 公有方法：启动添加噪声过程
    public void addNoise(double epsilon) {
        traverseAndAddNoise(root, 0, epsilon);
    }

    // 递归添加噪声（修改后）
    private void traverseAndAddNoise(Node node, int level, double epsilon) {
        if (node == null || level >= EPSILON_RATIOS.length) return;

        // 1. 计算当前层级的隐私预算
        double ratio = EPSILON_RATIOS[level];
        double epsilon_l = epsilon * ratio; // 按固定比例分配预算

        // 2. 计算Laplace噪声（灵敏度s=1）
        double lambda = 1.0 / epsilon_l;
        double noise = Math.round(generateLaplaceRandom(lambda));

        // 3. 添加噪声并处理负值
        node.pass += noise;
        if (node.pass < 0) node.pass = 0;

        // 4. 递归处理子节点
        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
            traverseAndAddNoise(entry.getValue(), level + 1, epsilon);
        }
    }

//    //平均分配
//    // 常量：最大深度（根据轨迹数据，最大长度为4，所以最大深度为4）
//    private static final int MAX_DEPTH = 4;
//
//    // 常量：层级总数（深度0~4）
//    private static final int TOTAL_LEVELS = 5;
//
//    // 公有方法：启动添加噪声过程
//    public void addNoise(double epsilon) {
//        traverseAndAddNoise(root, 0, epsilon);
//    }
//
//    // 递归添加噪声（修改后）
//    private void traverseAndAddNoise(Node node, int level, double epsilon) {
//        if (node == null || level >= TOTAL_LEVELS) return;
//
//        // 1. 计算当前层级的隐私预算（平均分配）
//        double epsilon_l = epsilon / TOTAL_LEVELS; // 每层获得20%的预算
//
//        // 2. 计算Laplace噪声（灵敏度s=1）
//        double lambda = 1.0 / epsilon_l;
//        double noise = Math.round(generateLaplaceRandom(lambda));
//
//        // 3. 添加噪声并处理负值
//        node.pass += noise;
//        if (node.pass < 0) node.pass = 0;
//
//        // 4. 递归处理子节点
//        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
//            traverseAndAddNoise(entry.getValue(), level + 1, epsilon);
//        }
//    }


    public static void generateWordsFromTrieToFile(Code02_TrieTree_RCDP trie,Code02_TrieTree_RCDP copy, String outputPath) {
        int totalCount = copy.root.pass; // 使用根节点的pass值作为总轨迹数
        if (totalCount <= 0) return;

        // 预计算数据结构
        class PrecomputedNode {
            int validPass;
            int childrenSum;
            double endProbability;
            List<String> keys = new ArrayList<>();
            List<Node> children = new ArrayList<>();
            List<Integer> cumulate = new ArrayList<>();
        }

        // 构建预计算映射
        Map<Node, PrecomputedNode> precomputedMap = new IdentityHashMap<>();
        Deque<Node> stack = new ArrayDeque<>();
        stack.push(trie.root);
        while (!stack.isEmpty()) {
            Node node = stack.pop();
            PrecomputedNode pn = new PrecomputedNode();
            pn.validPass = Math.max(0, node.pass);

            // 计算子节点信息
            int childSum = 0;
            if (node.nexts != null) {
                for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
                    Node child = entry.getValue();
                    if (!precomputedMap.containsKey(child)) {
                        stack.push(child);
                    }
                    int childPass = Math.max(0, child.pass);
                    if (childPass > 0) {
                        childSum += childPass;
                        pn.children.add(child);
                        pn.keys.add(entry.getKey());
                    }
                }
            }
            pn.childrenSum = childSum;
            pn.endProbability = (pn.validPass > 0 && pn.childrenSum > 0) ?
                    Math.max(0, (double)(pn.validPass - pn.childrenSum) / pn.validPass) : 0;

            // 构建累积分布
            int cum = 0;
            for (Node child : pn.children) {
                cum += Math.max(0, child.pass);
                pn.cumulate.add(cum);
            }
            precomputedMap.put(node, pn);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            Random random = new Random();
            StringBuilder sb = new StringBuilder();
            int batchSize = 10000; // 批量写入大小
            int count = 0;

            while (count < totalCount) {
                sb.setLength(0); // 复用StringBuilder
                Node current = trie.root;
                PrecomputedNode pn = precomputedMap.get(current);
                int depth = 0;
                boolean validTrajectory = false;

                while (depth < MAX_DEPTH) {
                    // 深度>=2时按概率结束
                    if (depth >= 2 && pn.endProbability > 0 && random.nextDouble() < pn.endProbability) {
                        validTrajectory = true;
                        break;
                    }

                    // 选择子节点
                    if (pn.children.isEmpty()) break;
                    int totalCum = pn.cumulate.get(pn.cumulate.size() - 1);
                    if (totalCum <= 0) break;

                    int randVal = random.nextInt(totalCum);

                    // 线性查找效率足够（因为每个节点的子节点数量有限，通常不超过10）
                    int idx = 0;
                    for (; idx < pn.cumulate.size(); idx++) {
                        if (randVal < pn.cumulate.get(idx)) {
                            break;
                        }
                    }

                    // 添加路径
                    if (sb.length() > 0) sb.append(',');
                    sb.append(pn.keys.get(idx));
                    current = pn.children.get(idx);
                    pn = precomputedMap.get(current);
                    depth++;
                    validTrajectory = (depth >= 2);
                }

                if (validTrajectory) {
                    writer.write(sb.toString());
                    writer.newLine();
                    count++;

                    // 每batchSize条刷新一次缓冲区
                    if (count % batchSize == 0) {
                        writer.flush();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //遍历所有节点存储内容
    public void traverse(Node node, String path) {

        if (node == null) return;
        // 打印节点信息
        System.out.println("Path: " + path + ", Pass: " + node.pass);
        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
            traverse(entry.getValue(), path + "," + entry.getKey());
        }
    }

    public void traverse() {
        traverse(root, "");
    }

    // 层次遍历并输出节点内容
    public void levelOrderTraversal() {
        if (root == null) return;
        List<Node> queue = new ArrayList<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Node currentNode = queue.remove(0);
            System.out.println("节点： " + currentNode.pass );
            for (Map.Entry<String, Node> entry : currentNode.nexts.entrySet()) {
                System.out.println("子节点： " + entry.getKey() + " -> " + entry.getValue().pass );
                queue.add(entry.getValue());
            }
        }
    }


    // 计算标准互信息
    public static double calculateNormalizedMutualInformation(Node root1, Node root2) {
        // 收集两棵树的所有节点值
        List<Integer> xValues = new ArrayList<>();
        List<Integer> yValues = new ArrayList<>();
        traverseTrees(root1, root2, xValues, yValues);

        // 计算互信息和熵
        return calculateNMI(xValues, yValues);
    }

    // 同时遍历两棵树并收集pass值（保持不变）
    private static void traverseTrees(Node node1, Node node2, List<Integer> xValues, List<Integer> yValues) {
        if (node1 == null || node2 == null) return;

        // 添加当前节点的值
        xValues.add(node1.pass);
        yValues.add(node2.pass);

        // 确保子节点顺序一致
        List<String> sortedKeys = new ArrayList<>(node1.nexts.keySet());
        Collections.sort(sortedKeys);

        for (String key : sortedKeys) {
            Node child1 = node1.nexts.get(key);
            Node child2 = node2.nexts.get(key);
            if (child1 != null && child2 != null) {
                traverseTrees(child1, child2, xValues, yValues);
            }
        }
    }

    // 计算标准互信息（NMI）
    private static double calculateNMI(List<Integer> xValues, List<Integer> yValues) {
        int n = xValues.size();
        if (n == 0) return 0.0;

        // 计算联合分布和边缘分布
        Map<String, Integer> jointCount = new HashMap<>();
        Map<Integer, Integer> xCount = new HashMap<>();
        Map<Integer, Integer> yCount = new HashMap<>();

        for (int i = 0; i < n; i++) {
            int x = xValues.get(i);
            int y = yValues.get(i);

            // 更新联合分布
            String jointKey = x + "," + y;
            jointCount.put(jointKey, jointCount.getOrDefault(jointKey, 0) + 1);

            // 更新边缘分布
            xCount.put(x, xCount.getOrDefault(x, 0) + 1);
            yCount.put(y, yCount.getOrDefault(y, 0) + 1);
        }

        // 计算互信息和熵
        double mi = 0.0;  // 互信息
        double hX = 0.0;  // X的熵
        double hY = 0.0;  // Y的熵

        // 计算X的熵
        for (int count : xCount.values()) {
            double p = (double) count / n;
            hX -= p * Math.log(p);  // 使用自然对数
        }

        // 计算Y的熵
        for (int count : yCount.values()) {
            double p = (double) count / n;
            hY -= p * Math.log(p);  // 使用自然对数
        }

        // 计算互信息
        for (Map.Entry<String, Integer> entry : jointCount.entrySet()) {
            String[] parts = entry.getKey().split(",");
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            double pXY = (double) entry.getValue() / n;
            double pX = (double) xCount.get(x) / n;
            double pY = (double) yCount.get(y) / n;

            mi += pXY * Math.log(pXY / (pX * pY));
        }

        // 计算标准互信息（NMI）
        // 使用公式: NMI = 2 * MI / (H(X) + H(Y))
        if (hX + hY == 0) {
            return 0.0;  // 避免除零错误
        }
        return 2 * mi / (hX + hY);
    }


    public void updateTree() {
        if (root == null) return;
        updateNode(root);
    }

    private void updateNode(Node node) {
        if (node.nexts == null || node.nexts.isEmpty()) {
            return;
        }

        int childPassSum = 0;
        List<Node> children = new ArrayList<>(node.nexts.values());

        // 确保子节点已更新
        for (Node child : children) {
            updateNode(child);
        }

        // 重新计算子节点之和（子节点可能已被更新）
        for (Node child : children) {
            childPassSum += child.pass; // 累加前确保child.pass>=0
        }

        // 只有当父节点计数 < 子节点之和时，才调整父节点
        if (node.pass < childPassSum) {
            // 父节点计数不足的情况：只需增加父节点计数至等于子节点之和
            node.pass = childPassSum;
        }
    }


}

