﻿//------------------------------------------------------------------------------
// <auto-generated>
//     Этот код создан программой.
//     Исполняемая версия:4.0.30319.34014
//
//     Изменения в этом файле могут привести к неправильной работе и будут потеряны в случае
//     повторной генерации кода.
// </auto-generated>
//------------------------------------------------------------------------------

// 
// This source code was auto-generated by xsd, Version=4.0.30319.33440.
// 
namespace fpNode.FIAS.Models {
    using System.Xml.Serialization;
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    [System.Xml.Serialization.XmlRootAttribute(Namespace="", IsNullable=false)]
    public partial class StructureStatuses {
        
        private StructureStatusesStructureStatus[] structureStatusField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("StructureStatus")]
        public StructureStatusesStructureStatus[] StructureStatus {
            get {
                return this.structureStatusField;
            }
            set {
                this.structureStatusField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.0.30319.33440")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    public partial class StructureStatusesStructureStatus {
        
        private string sTRSTATIDField;
        
        private string nAMEField;
        
        private string sHORTNAMEField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="integer")]
        public string STRSTATID {
            get {
                return this.sTRSTATIDField;
            }
            set {
                this.sTRSTATIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string NAME {
            get {
                return this.nAMEField;
            }
            set {
                this.nAMEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string SHORTNAME {
            get {
                return this.sHORTNAMEField;
            }
            set {
                this.sHORTNAMEField = value;
            }
        }
    }
}
