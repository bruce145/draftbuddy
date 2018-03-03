<%@ Page Title="Home Page" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Default.aspx.cs" Inherits="DraftBuddyWebApp._Default" %>

<asp:Content ID="BodyContent" ContentPlaceHolderID="MainContent" runat="server">

   <asp:ListView ID="playerList" runat="server" DataKeyNames="pid" ItemType="DraftBuddyWebApp.Models.FantasyStat2017" SelectMethod="GetPlayers" GroupItemCount="1">
       <EmptyDataTemplate>
                    <table >
                        <tr>
                            <td>No data was returned.</td>
                        </tr>
                    </table>
                </EmptyDataTemplate>
                <EmptyItemTemplate>
                    <td/>
                </EmptyItemTemplate>
       <GroupTemplate>
            <tr id="itemPlaceholderContainer" runat="server">
                <td id="itemPlaceholder" runat="server"></td>
            </tr>
        </GroupTemplate>
        <LayoutTemplate>
            <table style="width:100%;">
                <tbody>
                    <tr>
                        <td>
                            <table id="groupPlaceholderContainer" runat="server" style="width:100%">
                                <tr id="groupPlaceholder"></tr>
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td></td>
                    </tr>
                    <tr></tr>
                </tbody>
            </table>
        </LayoutTemplate>
        <ItemTemplate>
                    <td runat="server">
                        <table>
                            <td><%#:Item.Pid %></td>
                            <td><%#:Item.FirstName %></td>
                            <td><%#:Item.LastName%></td>
                            <td><%#:Item.GP %></td>
                            <td><%#:Item.Goals %></td>
                            <td><%#:Item.Assists %></td>
                        </table>
                        </p>
                    </td>
                </ItemTemplate>
    </asp:ListView>
</asp:Content>
