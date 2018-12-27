/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CustomizePathTraverser extends HugeTraverser {

    public CustomizePathTraverser(HugeGraph graph) {
        super(graph);
    }

    public List<Path> customizedPaths(List<HugeVertex> vertices,
                                      List<Step> steps, boolean sorted,
                                      long capacity, long limit) {
        E.checkArgument(!vertices.isEmpty(),
                        "The source vertices can't be empty");
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");
        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        for (HugeVertex vertex : vertices) {
            Node node = sorted ?
                        new WeightNode(vertex.id(), null, 0) :
                        new Node(vertex.id(), null);
            sources.add(vertex.id(), node);
        }
        int stepNum = steps.size();
        int pathCount = 0;
        long accessCount = 0;
        MultivaluedMap<Id, Node> newVertices = null;
        root : for (Step step : steps) {
            stepNum--;
            newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            Directions direction = Directions.valueOf(step.direction);
            // Traversal vertices of previous level
            for (List<Node> nodes : sources.values()) {
                for (Node n : nodes) {
                    for (String label : step.labels) {
                        Id el = this.getEdgeLabelId(label);
                        edges = edgesOfVertex(n.id(), direction,
                                              el, step.degree);
                        while (edges.hasNext()) {
                            HugeEdge edge = (HugeEdge) edges.next();
                            Id target = edge.id().otherVertexId();
                            // If have loop, skip target
                            if (n.contains(target)) {
                                continue;
                            }
                            Node newNode;
                            if (sorted) {
                                double weight = step.weightBy != null ?
                                                edge.value(step.weightBy) :
                                                step.defaultWeight;
                                newNode = new WeightNode(target, n, weight);
                            } else {
                                newNode = new Node(target, n);
                            }
                            // Add node to next start-nodes
                            newVertices.add(target, newNode);

                            if (stepNum == 0) {
                                if (limit != NO_LIMIT && !sorted &&
                                    ++pathCount >= limit) {
                                    break root;
                                }
                            }
                            if (capacity != NO_LIMIT &&
                                ++accessCount >= capacity) {
                                break root;
                            }
                        }
                    }
                }
            }
            // Re-init sources
            sources = newVertices;
        }
        if (stepNum != 0) {
            return ImmutableList.of();
        }
        List<Path> paths = new ArrayList<>();
        for (List<Node> nodes : newVertices.values()) {
            for (Node n : nodes) {
                if (sorted) {
                    WeightNode wn = (WeightNode) n;
                    paths.add(new WeightPath(null, wn.path(), wn.weights()));
                } else {
                    paths.add(new Path(null, n.path()));
                }
            }
        }
        return paths;
    }

    public static List<Path> topNPath(List<Path> paths,
                                      boolean incr, long limit) {
        if (limit == HugeTraverser.NO_LIMIT || paths.size() <= limit) {
            return paths;
        }

        paths.sort(((p1, p2) -> {
            CustomizePathTraverser.WeightPath wp1 = (CustomizePathTraverser.WeightPath) p1;
            CustomizePathTraverser.WeightPath wp2 = (CustomizePathTraverser.WeightPath) p2;
            int result;
            if (wp1.totalWeight() > wp2.totalWeight()) {
                result = 1;
            } else if (wp1.totalWeight() < wp2.totalWeight()) {
                result = -1;
            } else {
                result = 0;
            }
            if (incr) {
                return result;
            } else {
                return -result;
            }
        }));

        return paths.subList(0, (int) limit);
    }

    public static class WeightNode extends Node {

        private double weight;

        public WeightNode(Id id, Node parent, double weight) {
            super(id, parent);
            this.weight = weight;
        }

        public List<Double> weights() {
            List<Double> weights = new ArrayList<>();
            WeightNode current = this;
            while (current.parent() != null) {
                weights.add(current.weight);
                current = (WeightNode) current.parent();
            }
            Collections.reverse(weights);
            return weights;
        }
    }

    public static class WeightPath extends Path {

        private List<Double> weights;
        private double totalWeight;

        public WeightPath(Id crosspoint, List<Id> vertices,
                          List<Double> weights) {
            super(crosspoint, vertices);
            this.weights = weights;
            this.calcTotalWeight();
        }

        public List<Double> weights() {
            return this.weights;
        }

        public double totalWeight() {
            return this.totalWeight;
        }

        @Override
        public void reverse() {
            super.reverse();
            Collections.reverse(this.weights);
        }

        @Override
        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint(),
                                       "objects", this.vertices(),
                                       "weights", this.weights());
            } else {
                return ImmutableMap.of("objects", this.vertices(),
                                       "weights", this.weights());
            }
        }

        private void calcTotalWeight() {
            double sum = 0;
            for (double w : this.weights()) {
                sum += w;
            }
            this.totalWeight = sum;
        }
    }

    public static class Step {

        public String direction;
        public List<String> labels;
        @JsonProperty("weight_by")
        public String weightBy;
        @JsonProperty("default_weight")
        public double defaultWeight;
        public long degree;

        @Override
        public String toString() {
            return String.format("step:{direction=%s,labels=%s,weightBy=%s," +
                   "defaultWeight=%s,degree=%s}", this.direction, this.labels,
                   this.weightBy, this.defaultWeight, this.degree);
        }
    }
}
