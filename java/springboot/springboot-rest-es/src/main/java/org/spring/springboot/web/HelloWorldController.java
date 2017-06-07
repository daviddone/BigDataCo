package org.spring.springboot.web;

import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.spring.springboot.domain.Info;
import org.spring.springboot.es.esUtil;
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
    public Map<String, Object> searchInfo(@PathVariable("keyword") String keyword,HttpServletResponse resp) {
    	String index = "cq";
    	String type = "info";
    	System.out.println("keyword:"+keyword);
    	Map<String, Object> map = esUtil.search(keyword, index, type, 0, 5);
    	resp.setHeader("Access-Control-Allow-Origin", "*");
        return map;
    }
    
    @RequestMapping(value = "/api/search", method = RequestMethod.GET)
    public Info findOneCity() {
    	Info info = new Info("标题一", "描述一", "http://www.daviddone.com");
        return info;
    }
}
