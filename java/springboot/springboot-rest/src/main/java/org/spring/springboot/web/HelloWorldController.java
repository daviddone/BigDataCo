package org.spring.springboot.web;

import org.spring.springboot.domain.Info;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController //restful
public class HelloWorldController {

    @RequestMapping("/")
    public String sayHello() {
        return "Hello,World!";
    }
    
    @RequestMapping(value = "/api/search/{keyword}", method = RequestMethod.GET)
    public Info searchInfo(@PathVariable("keyword") String keyword) {
    	System.out.println("keyword:"+keyword);
    	Info info = new Info("标题一", "描述一"+keyword, "http://www.daviddone.com");
        return info;
    }
    
    @RequestMapping(value = "/api/search", method = RequestMethod.GET)
    public Info findOneCity() {
    	Info info = new Info("标题一", "描述一", "http://www.daviddone.com");
        return info;
    }
}
