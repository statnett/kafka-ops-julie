package com.purbon.kafka.topology.model.users;

import com.purbon.kafka.topology.model.User;
import java.util.List;

public class Schemas extends User {

  private List<String> subjects;
  private String role;
  private boolean prefixed;

  public Schemas() {
    super("");
    // using default role RESOURCE_OWNER and non-prefixed (literal) binding
    // for backward compatibility
    this.role = "ResourceOwner";
    this.prefixed = false;
  }

  public List<String> getSubjects() {
    return subjects;
  }

  public void setSubjects(List<String> subjects) {
    this.subjects = subjects;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public boolean isPrefixed() {
    return prefixed;
  }

  public void setPrefixed(boolean prefixed) {
    this.prefixed = prefixed;
  }
}
