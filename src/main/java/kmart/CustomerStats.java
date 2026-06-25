package kmart;

import java.io.Serializable;

public class CustomerStats implements Serializable {

    public String customerId;
    public Integer purchaseCount;
    public Double totalSpent;

    public CustomerStats() {
    }

    public CustomerStats(
            String customerId,
            Integer purchaseCount,
            Double totalSpent) {

        this.customerId = customerId;
        this.purchaseCount = purchaseCount;
        this.totalSpent = totalSpent;
    }
}