package dev.lukasgrigis.outboxinbox.loyalty.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface StampRepository extends JpaRepository<Stamp, Long> {

}
