/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package io.hops.hopsworks.expat.migrations.featurestore.featuregroup;

import io.hops.hopsworks.expat.migrations.projects.search.featurestore.FeaturestoreXAttrsConstants;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * features of featuregroups turn from being strings to objects of {name, description}
 */
public class FeaturegroupXAttrV2 {
  /**
   * common fields
   */
  public static abstract class Base {
    @XmlElement(nillable = false, name = FeaturestoreXAttrsConstants.FEATURESTORE_ID)
    private Integer featurestoreId;
    
    public Base() {}
    
    public Base(Integer featurestoreId) {
      this.featurestoreId = featurestoreId;
    }
    
    public Integer getFeaturestoreId() {
      return featurestoreId;
    }
    
    public void setFeaturestoreId(Integer featurestoreId) {
      this.featurestoreId = featurestoreId;
    }
    
    @Override
    public String toString() {
      return "Base{" +
        "featurestoreId=" + featurestoreId +
        '}';
    }
  }
  
  /**
   * document attached as an xattr to a featuregroup directory
   */
  @XmlRootElement
  public static class FullDTO extends Base {
    @XmlElement(nillable = true, name = FeaturestoreXAttrsConstants.DESCRIPTION)
    private String description;
    @XmlElement(nillable = true, name = FeaturestoreXAttrsConstants.CREATE_DATE)
    private Long createDate;
    @XmlElement(nillable = true, name = FeaturestoreXAttrsConstants.CREATOR)
    private String creator;
    @XmlElement(nillable = false, name = FeaturestoreXAttrsConstants.FG_FEATURES)
    private List<SimpleFeatureDTO>
      features = new LinkedList<>();
    
    public FullDTO() {
      super();
    }
    
    public FullDTO(Integer featurestoreId, String description, Long createDate, String creator) {
      this(featurestoreId, description, createDate, creator, new LinkedList<>());
    }
    
    public FullDTO(Integer featurestoreId, String description,
      Long createDate, String creator, List<SimpleFeatureDTO> features) {
      super(featurestoreId);
      this.features = features;
      this.description = description;
      this.createDate = createDate;
      this.creator = creator;
    }
    
    public String getDescription() {
      return description;
    }
    
    public void setDescription(String description) {
      this.description = description;
    }
    
    public Long getCreateDate() {
      return createDate;
    }
    
    public void setCreateDate(Long createDate) {
      this.createDate = createDate;
    }
    
    public String getCreator() {
      return creator;
    }
    
    public void setCreator(String creator) {
      this.creator = creator;
    }
    
    public List<SimpleFeatureDTO> getFeatures() {
      return features;
    }
    
    public void setFeatures(List<SimpleFeatureDTO> features) {
      this.features = features;
    }
    
    public void addFeature(SimpleFeatureDTO feature) {
      features.add(feature);
    }
    
    public void addFeatures(List<SimpleFeatureDTO> features) {
      this.features.addAll(features);
    }
    
    @Override
    public String toString() {
      return super.toString() + "Extended{" +
        "description='" + description + '\'' +
        ", createDate=" + createDate +
        ", creator='" + creator + '\'' +
        ", features=" + features +
        '}';
    }
  }
  
  @XmlRootElement
  public static class SimpleFeatureDTO {
    private String name;
    private String description;
    
    public SimpleFeatureDTO() {}

    public SimpleFeatureDTO(String name) {
      this.name = name;
    }

    public SimpleFeatureDTO(String name, String description) {
      this.name = name;
      this.description = description;
    }
    
    public String getName() {
      return name;
    }
    
    public void setName(String name) {
      this.name = name;
    }
    
    public String getDescription() {
      return description;
    }
    
    public void setDescription(String description) {
      this.description = description;
    }
    
    @Override
    public String toString() {
      return "SimpleFeatureDTO{" +
        "name='" + name + '\'' +
        ", description='" + description + '\'' +
        '}';
    }
  }
  
  public static String jaxbMarshal(JAXBContext jaxbContext, FullDTO xattr) throws JAXBException {
    Marshaller marshaller = jaxbContext.createMarshaller();
    StringWriter sw = new StringWriter();
    marshaller.marshal(xattr, sw);
    return sw.toString();
  }
  
  public static FullDTO jaxbUnmarshal(JAXBContext jaxbContext, byte[] val) throws JAXBException {
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    StreamSource ss = new StreamSource(new StringReader(new String(val)));
    return unmarshaller.unmarshal(ss, FullDTO.class).getValue();
  }
}

