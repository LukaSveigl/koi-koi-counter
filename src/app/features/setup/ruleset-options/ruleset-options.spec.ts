import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RulesetOptions } from './ruleset-options';

describe('RulesetOptions', () => {
  let component: RulesetOptions;
  let fixture: ComponentFixture<RulesetOptions>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RulesetOptions],
    }).compileComponents();

    fixture = TestBed.createComponent(RulesetOptions);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
