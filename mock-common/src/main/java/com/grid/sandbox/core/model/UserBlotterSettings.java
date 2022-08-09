package com.grid.sandbox.core.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.data.domain.Sort;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class UserBlotterSettings {
    private Sort sort;
    private String filterString;
}
