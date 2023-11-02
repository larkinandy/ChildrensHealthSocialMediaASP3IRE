
import React from 'react';
import "./styles.css";
import * as Survey from "survey-react";
import "survey-react/survey.css";

class ChildSurvey extends React.Component {
    constructor(props) {
      super(props);
      this.state = { 
        isCompleted:false
       };
      this.onCompleteComponent = this.onCompleteComponent.bind(this);
    }



    onCompleteComponent = (props) =>  {
      this.props.onFinish(props.data);
      this.model.clear();
    }


    render() {
      var defaultQuestions =  [
        {
        "type": "boolean",
            "name": "isChild",
            "title": "Please answer the question",
            "label": "Does the tweet mention or depict a child or children?",
            "isRequired": true
        },
        {
          type: "checkbox",
          name: "age",
          title: "What age group does the child(ren) belong to (can select multiple)?",
          isRequired: false,
          hasSelectAll: false,
          hasNone: false,
          noneText: "None of the above",
          colCount: 3,
          visibleIf: "{isChild}==true",
          choices: [
           "0 to less than 1 year (baby/infant)",
           "1 to 4 years (toddler/pre-school)",
           "5 to 10 years (elementary school)",
           "11 to 13 years (middle school)",
           "14 to 17 years (high school)",
           "School age (no specific school type)",
           "No specific age (children general)",
           "Unsure"
          ]
        }
      ]
     
      this.model = new Survey.Model({questions: defaultQuestions});

      var surveyRender = (
        <Survey.Survey
         model={this.model}
          showCompletedPage={false}
          onComplete={this.onCompleteComponent}
        />
      );
  
      return (
        <div>
          {surveyRender} 
        </div>
      );
    }
  }

  
export default ChildSurvey;