package io.github.kaulinmindiola.rop.system.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Read-only access for assertions. Connects as the database superuser because a test needs to see
 * both schemas; the services themselves keep using their own roles (ADR-0002, ADR-0015).
 */
public final class SystemDatabase {

    private final String url;
    private final String user;
    private final String password;

    public SystemDatabase(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public int stockOf(String productId) {
        return queryInt(
                "SELECT quantity FROM inventory_service.stock WHERE product_id = ?", productId);
    }

    public int queryInt(String sql, Object parameter) {
        try (Connection connection = DriverManager.getConnection(url, user, password);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, parameter);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("No row for: " + sql);
                }
                return result.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Assertion query failed: " + sql, e);
        }
    }
}
