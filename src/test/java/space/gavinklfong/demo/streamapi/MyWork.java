package space.gavinklfong.demo.streamapi;

import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import lombok.extern.slf4j.Slf4j;
import space.gavinklfong.demo.streamapi.models.Customer;
import space.gavinklfong.demo.streamapi.models.Order;
import space.gavinklfong.demo.streamapi.models.Product;
import space.gavinklfong.demo.streamapi.repos.CustomerRepo;
import space.gavinklfong.demo.streamapi.repos.OrderRepo;
import space.gavinklfong.demo.streamapi.repos.ProductRepo;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
public class MyWork {

    Logger log = LoggerFactory.getLogger(StreamApiTest.class);
    @Autowired
    private CustomerRepo customerRepo;

    @Autowired
    private OrderRepo orderRepo;

    @Autowired
    private ProductRepo productRepo;

    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100")
    public void exercise1() {
      List<Product> productList = productRepo.findAll();
        List<Product> result = productList.stream()
                .filter(p -> p.getCategory().equalsIgnoreCase("Books"))
                .filter(p -> p.getPrice() > 100)
              .collect(Collectors.toList());

        System.out.println("Result = " + result);
        result.forEach(product -> {
            assertEquals("Books", product.getCategory());
            assertTrue(product.getPrice() > 100);
        });
    }

    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100 (using Predicate chaining for filter)")
    public void exercise1a() {
        List<Product> productList = productRepo.findAll();

        Predicate<Product> condition1 = product -> product.getCategory().equalsIgnoreCase("Books");
        Predicate<Product> condition2 = product -> product.getPrice() > 100;

        List<Product> result = productList.stream()
                .filter(condition1.and(condition2))
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
        result.forEach(product -> {
            assertEquals("Books", product.getCategory());
            assertTrue(product.getPrice() > 100);
        });
    }

    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100 (using BiPredicate for filter)")
    public void exercise1b() {
        List<Product> productList = productRepo.findAll();

        BiPredicate<Product, String> categoryFilter = (product, category) -> product.getCategory().equalsIgnoreCase(category);
        BiPredicate<Product, Integer> priceFilter = (product, price) -> product.getPrice() > price;

        List<Product> result = productList.stream()
                .filter(product -> categoryFilter.test(product, "Books") && priceFilter.test(product, 100))
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
        result.forEach(product -> {
            assertEquals("Books", product.getCategory());
            assertTrue(product.getPrice() > 100);
        });

    }

    @Test
    @DisplayName("Obtain a list of order with product category = \"Baby\"")
    public void exercise2() {
        List<Order> orderList = orderRepo.findAll();
        List<Order> result = orderList.stream()
                .filter(order -> order.getProducts().stream()
                        .anyMatch(product -> product.getCategory().equals("Baby")))
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
        result.forEach(order -> {
            boolean flag = false;
            for (Product product : order.getProducts()) {
                if (product.getCategory().equals("Baby")) flag = true;
            }
            assertTrue(flag, "failed for order id = " + order.getId());
        });

    }

    @Test
    @DisplayName("Obtain a list of product with category = “Toys” and then apply 10% discount\"")
    public void exercise3() {
        List<Product> productList = productRepo.findAll();
        List<Product> result = productList
                .stream()
                .filter(product -> product.getCategory().equals("Toys"))
                .map(product -> product.withPrice(product.getPrice()*0.9))
                .collect(Collectors.toList());

        System.out.println("Result = " + result);

        result.forEach(product -> {
            assertEquals("Toys", product.getCategory());
        });
    }

    @Test
    @DisplayName("Obtain a list of products ordered by customer of tier 2 between 01-Feb-2021 and 01-Apr-2021")
    public void exercise4() {
        List<Order> orderList = orderRepo.findAll();
        List<Product> result = orderList
                .stream()
                .filter(o -> o.getCustomer().getTier() == 2)
                .filter(o -> LocalDate.of(2021,02,01).compareTo(o.getOrderDate()) <= 0
                        && LocalDate.of(2021,04,01).compareTo(o.getOrderDate()) >= 0)
                .map(Order::getProducts)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
        String expResults = "[Product(id=10, name=eos sed debitis, category=Baby, price=366.9), Product(id=25, name=magnam adipisci voluptate, category=Grocery, price=366.13), Product(id=8, name=deleniti earum et, category=Baby, price=41.46), Product(id=13, name=sint voluptatem ut, category=Toys, price=295.37), Product(id=1, name=omnis quod consequatur, category=Games, price=184.83), Product(id=21, name=consectetur cupiditate sunt, category=Toys, price=95.46), Product(id=12, name=ut perferendis corporis, category=Grocery, price=302.19), Product(id=14, name=quos sunt ipsam, category=Grocery, price=534.64), Product(id=18, name=aut accusamus quia, category=Baby, price=881.38), Product(id=4, name=voluptatem voluptas aspernatur, category=Toys, price=536.8), Product(id=27, name=dolores ipsum sit, category=Toys, price=786.99), Product(id=19, name=doloremque incidunt sed, category=Games, price=988.49), Product(id=6, name=dolorem porro debitis, category=Toys, price=146.52), Product(id=22, name=itaque ea qui, category=Baby, price=677.78), Product(id=11, name=laudantium sit nihil, category=Toys, price=95.5), Product(id=15, name=qui illo error, category=Baby, price=623.58), Product(id=20, name=libero omnis velit, category=Baby, price=177.61), Product(id=24, name=veniam consequatur et, category=Books, price=893.44), Product(id=26, name=reiciendis consequuntur placeat, category=Toys, price=359.27)]";
        assertEquals(expResults, result.toString());

    }

    @Test
    @DisplayName("Get the 3 cheapest products of \"Books\" category")
    public void exercise5() {
        List<Product> productList = productRepo.findAll();
        List<Product> result = productList
                .stream()
                .filter(product -> product.getCategory().equals("Books"))
                .sorted(Comparator.comparing(Product::getPrice))
                .limit(3)
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Get the 3 most recent placed order")
    public void exercise6() {
        List<Order> orderList = orderRepo.findAll();
        List<Order> result = orderList
                .stream()
                .sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .limit(3)
                .collect(Collectors.toList());

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Get a list of products which was ordered on 15-Mar-2021")
    public void exercise7() {
        List<Order> orderList = orderRepo.findAll();
        List<Product> result = orderList
                .stream()
                .filter(order -> order.getOrderDate().equals(LocalDate.of(2021,3,15)))
                .flatMap(order -> order.getProducts().stream())
                .distinct()
                .collect(Collectors.toList());

        System.out.println("Result = " + result);

    }

    @Test
    @DisplayName("Calculate the total lump of all orders placed in Feb 2021")
    public void exercise8() {
        List<Order> orderList = orderRepo.findAll();
        Double result = orderList
                .stream()
                .filter(order -> order.getOrderDate().getYear() == 2021
                        && order.getOrderDate().getMonth().getValue() == 2)
                .flatMap(order -> order.getProducts().stream())
                .mapToDouble(Product::getPrice)
                .sum();

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Calculate the total lump of all orders placed in Feb 2021 (using reduce with BiFunction)")
    public void exercise8a() {
        List<Order> orderList = orderRepo.findAll();
        Double result = orderList
                .stream()
                .filter(order -> order.getOrderDate().getYear() == 2021
                        && order.getOrderDate().getMonth().getValue() == 2)
                .flatMap(order -> order.getProducts().stream())
                .map(Product::getPrice)
                .reduce(Double::sum)
                .orElse(0d);

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Calculate the average price of all orders placed on 15-Mar-2021")
    public void exercise9() {
        List<Order> orderList = orderRepo.findAll();
        Double result = orderList
                .stream()
                .filter(order -> order.getOrderDate().equals(LocalDate.of(2021,3,15)))
                .flatMap(order -> order.getProducts().stream())
                .mapToDouble(Product::getPrice)
                .average()
                .orElse(0d);

        System.out.println("Result = " + result);

    }

    @Test
    @DisplayName("Obtain statistics summary of all products belong to \"Books\" category")
    public void exercise10() {
        List<Product> productList = productRepo.findAll();
        DoubleSummaryStatistics result = productList
                .stream()
                .filter(product -> product.getCategory().equals("Books"))
                .mapToDouble(Product::getPrice)
                .summaryStatistics();

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a mapping of order id and the order's product count")
    public void exercise11() {
        List<Order> orderList = orderRepo.findAll();
        Map<Long, Integer> result = orderList
                .stream()
                .collect(
                        Collectors.toMap(
                                Order::getId,
                                order -> order.getProducts().size()
                        )
                );

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a data map of customer and list of orders")
    public void exercise12() {
        List<Order> orderList = orderRepo.findAll();
        Map<Customer, List<Order>> result = orderList
                .stream()
                .collect(Collectors
                        .groupingBy(
                                Order::getCustomer
                        )
                );

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a data map of customer_id and list of order_id(s)")
    public void exercise12a() {
        List<Order> orderList = orderRepo.findAll();
        Map<Long, List<Long>> result = orderList
                .stream()
                .collect(Collectors.groupingBy(
                        order -> order.getCustomer().getId(),
                        HashMap::new,
                        Collectors.mapping(Order::getId, Collectors.toList())
                ));

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a data map with order and its total price")
    public void exercise13() {
        List<Order> orderList = orderRepo.findAll();
        Map<Order, Double> result = orderList
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        order -> order.getProducts()
                                .stream()
                                .mapToDouble(Product::getPrice)
                                .sum()
                ));

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a data map with order and its total price (using reduce)")
    public void exercise13a() {
        List<Order> orderList = orderRepo.findAll();
        Map<Order, Double> result = orderList
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        order -> order.getProducts()
                                .stream()
                                .map(Product::getPrice)
                                .reduce(Double::sum)
                                .orElse(0d)
                ));

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Obtain a data map of product name by category")
    public void exercise14() {
        List<Product> productList = productRepo.findAll();
        Map<String, List<String>> result = productList
                .stream()
                .collect(Collectors.groupingBy(
                        Product::getCategory,
                        HashMap::new,
                        Collectors.mapping(Product::getName, Collectors.toList())
                ));

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Get the most expensive product per category")
    void exercise15() {
        List<Product> productList = productRepo.findAll();
        Map<String, Optional<Product>> result = productList
                .stream()
                .collect(Collectors.groupingBy(
                        Product::getCategory,
                        Collectors.maxBy(Comparator.comparing(Product::getPrice))
                ));

        System.out.println("Result = " + result);
    }

    @Test
    @DisplayName("Get the most expensive product (by name) per category")
    void exercise15a() {
        List<Product> productList = productRepo.findAll();
        Map<String, Optional<String>> result =
                productList
                .stream()
                .collect(Collectors.groupingBy(
                        Product::getCategory,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(Comparator.comparing(Product::getPrice)),
                                product -> product.map(Product::getName))
                ));

        System.out.println("Result = " + result);
    }

}
