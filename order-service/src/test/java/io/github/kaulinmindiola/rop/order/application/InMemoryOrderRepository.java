package io.github.kaulinmindiola.rop.order.application;

import io.github.kaulinmindiola.rop.order.domain.model.Order;
import io.github.kaulinmindiola.rop.order.domain.port.OrderRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** In-memory fake of the order port for use case unit tests. */
final class InMemoryOrderRepository implements OrderRepository {

    private final Map<UUID, Order> orders = new HashMap<>();
    private int saves;

    @Override
    public void save(Order order) {
        orders.put(order.id(), order);
        saves++;
    }

    @Override
    public Optional<Order> findById(UUID id) {
        return Optional.ofNullable(orders.get(id));
    }

    int size() {
        return orders.size();
    }

    int saves() {
        return saves;
    }
}
