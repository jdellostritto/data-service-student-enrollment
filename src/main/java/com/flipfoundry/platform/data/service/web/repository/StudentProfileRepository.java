package com.flipfoundry.platform.data.service.web.repository;

import com.flipfoundry.platform.data.service.web.model.StudentProfile;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

/**
 * Repository for StudentProfile entity.
 */
@Repository
public interface StudentProfileRepository extends CassandraRepository<StudentProfile, String> {
}
