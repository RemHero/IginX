/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.GetUserReq;
import cn.edu.tsinghua.iginx.thrift.GetUserResp;
import java.util.List;

public class ShowUserStatement extends SystemStatement {

  private final List<String> users;

  public ShowUserStatement(List<String> users) {
    this.statementType = StatementType.SHOW_USER;
    this.users = users;
  }

  public List<String> getUsers() {
    return users;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    IginxWorker worker = IginxWorker.getInstance();
    GetUserReq req = new GetUserReq(ctx.getSessionId());
    if (users != null && !users.isEmpty()) {
      req.setUsernames(users);
    }
    GetUserResp getUserResp = worker.getUser(req);
    Result result = new Result(getUserResp.getStatus());
    result.setAuths(getUserResp.getAuths());
    result.setUsernames(getUserResp.getUsernames());
    result.setUserTypes(getUserResp.getUserTypes());

    ctx.setResult(result);
  }
}