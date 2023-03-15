# Created by amnon at 14/03/2023
Feature: Spreadsheet from a project with multiple submissions
  # Enter feature description here

  Scenario: Spreadsheet upload then add new file using api
    Given I create a submission
    And upload spreadsheet submission1.xlsx
    And set submission state to Exported
    And I create a new submission on the same project
    And add a new file called new_file.txt with type sequence_file

    When I export a spreadsheet

    Then spreadsheet contains a file called new_file.txt with type sequence_file
#    And spreadsheet contains all entities in the original submission