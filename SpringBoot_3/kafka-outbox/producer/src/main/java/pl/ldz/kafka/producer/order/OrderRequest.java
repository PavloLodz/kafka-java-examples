package pl.ldz.kafka.producer.order;

public record OrderRequest(String itemName, int quantity) {}
