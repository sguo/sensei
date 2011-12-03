package com.sensei.search.client.json.req;

import java.util.List;

import org.json.JSONObject;

import com.sensei.search.client.json.CustomJsonHandler;
import com.sensei.search.client.json.JsonField;
import com.sensei.search.client.json.req.filter.Filter;

@CustomJsonHandler(SelectionJsonHandler.class)
public abstract class Selection implements Filter {
    private String field;

    public String getField() {
        return field;
    }
    public Selection setField(String field) {
        this.field = field;
        return this;
    }
    public static class Path extends Selection {

        private String value;
        private boolean strict;
        private int depth;
        public Path(String value, boolean strict, int depth) {
            super();

            this.value = value;
            this.strict = strict;
            this.depth = depth;
        }

        public Path() {

        }
    }
    /**
     *     <p>Matches documents with fields that have terms within a certain range. The type of the Sensei query depends on the field type, for <code>string</code> fields, the <code>TermRangeQuery</code>, while for number/date fields, the query is a <code>NumericRangeQuery</code>. The following example returns all documents where <code>age</code> is between <code>10</code> and <code>20</code>:</p>

<pre class="prettyprint lang-js"><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="str">"range"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"age"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="pun">{</span><span class="pln"> <br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"from"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">10</span><span class="pun">,</span><span class="pln"> <br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"to"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">20</span><span class="pun">,</span><span class="pln"> <br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"include_lower"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="kwd">true</span><span class="pun">,</span><span class="pln"> <br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"include_upper"</span><span class="pun">:</span><span class="pln"> </span><span class="kwd">false</span><span class="pun">,</span><span class="pln"> <br>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; </span><span class="str">"boost"</span><span class="pln"> </span><span class="pun">:</span><span class="pln"> </span><span class="lit">2.0</span><span class="pln"><br>&nbsp; &nbsp; &nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br>&nbsp; &nbsp; </span><span class="pun">}</span><span class="pln"><br></span><span class="pun">}</span></pre>

<p>The <code>range</code> query top level parameters include:</p>
<table>
    <tbody><tr>
        <th>Name </th>
        <th>Description </th>
    </tr>
    <tr>

        <td> <code>from</code> </td>
        <td> The lower bound. Defaults to start from the first.</td>
    </tr>
    <tr>
        <td> <code>to</code> </td>

        <td> The upper bound. Defaults to unbounded. </td>
    </tr>
    <tr>
        <td> <code>include_lower</code> </td>
        <td> Should the first from (if set) be inclusive or not. Defaults to <code>true</code> </td>

    </tr>
    <tr>
        <td> <code>include_upper</code> </td>
        <td> Should the last to (if set) be inclusive or not. Defaults to <code>true</code>. </td>
    </tr>




     *
     */
    public static class Range extends Selection {

        private String upper;
        private String lower;
        @JsonField("include_lower")
        private boolean includeLower;
        @JsonField("include_upper")
        private boolean includeUpper;
       public Range() {
       }
    public Range(String upper, String lower, boolean includeUpper, boolean includeLower) {
        super();

        this.upper = upper;
        this.lower = lower;
        this.includeUpper = includeUpper;
        this.includeLower = includeLower;
    }

    }
    public static class Custom extends Selection {
        private JSONObject custom;

        public Custom(JSONObject custom) {
            super();
            this.custom = custom;
        }
        public Custom() {
            // TODO Auto-generated constructor stub
        }
        public JSONObject getCustom() {
            return custom;
        }
    }
    public static Selection term(String field, String value) {
        return new Term(value).setField(field);
    }
    public static Selection terms(String field, List<String> values, List<String> excludes, Operator op) {
        return new Terms(values,excludes, op).setField(field);
    }
    public static Selection range(String field, String upper, String lower, boolean includeUpper, boolean includeLower) {
         return new Range(upper, lower, includeUpper, includeLower).setField(field);
    }
    public static Selection range(String field, String upper, String lower) {
        return new Range(upper, lower, true, true).setField(field);
   }
    public static Selection path(String field, String value, boolean strict, int depth) {
        return new Path(value, strict, depth).setField(field);
    }
    public static Selection custom(JSONObject custom) {

        return new Custom(custom);
    }
}
