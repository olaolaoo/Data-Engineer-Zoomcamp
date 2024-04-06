package org.example;

public class Secrets {
    public static final String KAFKA_CLUSTER_KEY = "4F3RIFTOXTZG3XL5";
    public static final String KAFKA_CLUSTER_SECRET = "fjxHIvgf4cT2NaA1q4COWz02ufqm9xgOf0ZYdXFp17Xxoa5N78W1A3vAYvbDXdpR";

    public static final String SCHEMA_REGISTRY_KEY = "REPLACE_WITH_SCHEMA_REGISTRY_KEY";
    public static final String SCHEMA_REGISTRY_SECRET = "REPLACE_WITH_SCHEMA_REGISTRY_SECRET";

    public static void main(String[] args) {
        System.out.println(KAFKA_CLUSTER_KEY);
    }
}
