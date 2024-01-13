package com.example.emplyeemanagment.service;

import java.util.List;
import java.util.Optional;

import com.example.emplyeemanagment.entity.EmployeeEntity;

public interface EmployeeService {
	List<EmployeeEntity> findAllEmployee();
    Optional<EmployeeEntity> findById(Long id);
    EmployeeEntity saveEmployee(EmployeeEntity employeeEntity);
    EmployeeEntity updateEmployee(EmployeeEntity employeeEntity);
    void deleteEmployee(Long id);

//    Using Request for Save and Update Employee
//    EmployeeResponse saveEmployee(EmployeeRequest employeeRequest);
//    EmployeeResponse updateEmployee(EmployeeRequest employeeRequest, Long id);


}
