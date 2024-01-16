package consumer;

import java.util.List;
import java.util.Optional;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.emplyeemanagment.entity.EmployeeEntity;
import com.example.emplyeemanagment.service.EmployeeService;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final MessageService messageservice;

    public KafkaController(MessageService messageservice) {
        this.messageservice = messageservice;
    }

//    @PostMapping
//    public EmployeeEntity saveEmployee(@RequestBody EmployeeEntity employeeEntity) {
//        return employeeService.saveEmployee(employeeEntity);
//    }


}
