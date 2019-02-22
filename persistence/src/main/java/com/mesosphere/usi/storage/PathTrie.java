package com.mesosphere.usi.storage;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOTE: This is a copy of Zookeeper's [[org.apache.zookeeper.common.PathTrie]] class modified to provide additional
 * access to node's children and extending them with optional byte array field.
 *
 * Original description: a class that implements prefix matching for
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       /
 *      (bc)
 *   cf/
 *   (cf)
 *
 * @see <a href="http://people.apache.org/~larsgeorge/zookeeper-1215258/build/docs/dev-api/org/apache/zookeeper/common/PathTrie.html">PathTrie</a>
 */
public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);

    /**
     * the root node of PathTrie
     */
    private final TrieNode rootNode;

    public static class TrieNode {
        final HashMap<String, TrieNode> children;
        TrieNode parent = null;
        byte[] data = null;

        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        private TrieNode(TrieNode parent) {
            this(parent, null);
        }

        /**
         * create a trienode with parent and data
         *
         * @param parent the parent of this node
         * @param data this node's data
         */
        private TrieNode(TrieNode parent, byte[] data) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
            this.data = data;
        }

        /**
         * get the parent of this node
         * @return the parent node
         */
        synchronized TrieNode getParent() {
            return this.parent;
        }

        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        synchronized void setParent(TrieNode parent) {
            this.parent = parent;
        }

        /**
         * get this node's data.
         * @return byte array with this node's data or null otherwise
         */
        synchronized public byte[] getData() { return data; }

        /**
         * set this node's data
         * @param data of this node
         */
        synchronized public void setData(byte[] data) { this.data = data; }

        /**
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        synchronized void addChild(String childName, TrieNode node) {
            if (children.containsKey(childName)) {
                return;
            }
            children.put(childName, node);
        }

        /**
         * delete child from this node
         * @param childName the string name of the child to
         * be deleted
         */
        synchronized void deleteChild(String childName) {
            if (!children.containsKey(childName)) {
                return;
            }
            TrieNode childNode = children.get(childName);
            childNode.setParent(null);
            children.remove(childName);
        }

        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        synchronized TrieNode getChild(String childName) {
            if (!children.containsKey(childName)) {
                return null;
            }
            else {
                return children.get(childName);
            }
        }

        /**
         * get the list of children of this
         * trienode.
         * @return the string list of its children
         */
        synchronized String[] getChildren() {
            return children.keySet().toArray(new String[0]);
        }

        /**
         * get the map of children nodes for this trienode. An unmodifiable map is returned to prevent
         *
         * @return
         */
        synchronized Map<String, TrieNode> getChildrenNodes() {
            return Collections.unmodifiableMap(children);
        }

        /**
         * returns true if this is a leaf node. A leaf node doesn't have children and is *not* root node.
         *
         * @return true if this is a leaf node
         */
        synchronized Boolean isLeafNode() {
            return children.isEmpty() && parent != null;
        }

        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }

    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }

    public void addPath(String path) { addPath(path, null); }

    /**
     * add a path to the path trie
     * @param path
     */
    public void addPath(String path, byte[] data) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                parent.addChild(part, new TrieNode(parent));
            }
            parent = parent.getChild(part);
        }
        if (data != null) {
            parent.setData(data);
        }
    }

    /**
     * delete a path from the trie
     * @param path the path to be deleted
     */
    public void deletePath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                //the path does not exist
                LOG.warn("Can't delete path {} since it doesn't exist.", path);
                return;
            }
            parent = parent.getChild(part);
            LOG.info("{}",parent);
        }
        TrieNode realParent  = parent.getParent();
        realParent.deleteChild(part);
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for(String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }

    /**************************************************************************
     *                           Additional methods                           *
     **************************************************************************/

    /**
     * return trie's root node. Useful when iterating through the trie.
     *
     * @return root node
     */
    TrieNode getRoot() {
        return rootNode;
    }

    /**
     * return a trie's node for the given path.
     *
     * @param path input path
     * @return node with the given path or null if the path doesn't exist
     */
    TrieNode getNode(String path) {
        if (path == null) {
            return null;
        }
        if ("/".equals(path)) {
            return rootNode;
        }
        String[] pathComponents = path.split("/");
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        TrieNode parent = rootNode;
        String part = null;
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                //the path does not exist
                return null;
            }
            parent = parent.getChild(part);
            LOG.debug("{}",parent);
        }
        return parent;
    }

    /**
     * returns node data if node exists or null otherwise
     *
     * @param path input path
     * @return node data
     */
    public byte[] getNodeData(String path) {
        TrieNode node = getNode(path);
        if (node != null) {
            return node.getData();
        } else {
            return null;
        }
    }

    /**
     * returns true if a node with a given path exists in the trie
     *
     * @param path input path
     * @return true if the node exists, false otherwise
     */
    public Boolean existsNode(String path) {
        TrieNode node = getNode(path);
        return node != null;
    }

    private void getLeafs(String path, TrieNode node, Set<String> leafs) {
        if (node.isLeafNode()) {                 // If found leaf node (that is not root node)
            leafs.add(path);                     // add it to the set
        } else {                                 // else search all the children recursively
            node.getChildrenNodes()
                    .forEach((cp, n) ->
                            getLeafs(Paths.get(path, cp).toString(), n, leafs));
        }
    }

    /**
     * return leaf nodes recursively starting with the given path.
     *
     * @param path input path
     * @return a map with leaf nodes
     */
    public Set<String> getLeafs(String path) {
        TrieNode node = getNode(path);
        if (node == null) {
            return null;
        }

        Set<String> set = new HashSet<>();
        getLeafs(path, node, set);
        return set;
    }

    /**
     * return the children names for the given path. If the path doesn't exist,
     * null is returned
     *
     * @param path the input path
     * @param fullPath if true, full path for the children node will be returned
     * @return a set of children names
     */
    public Set<String> getChildren(String path, Boolean fullPath) {
        TrieNode node = getNode(path);
        if (node == null) {
            return null;
        }
        if (!fullPath){
            return new HashSet<>(Arrays.asList(node.getChildren()));
        } else {
            return Arrays.asList(node.getChildren())
                    .stream()
                    .map(p -> Paths.get(path, p).toString())
                    .collect(Collectors.toSet());
        }
    }
}
